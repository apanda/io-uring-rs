use clap::{Parser, Subcommand};
use io_uring::{opcode, squeue::Entry, types, IoUring};
use nix::errno;
use nix::libc::{iovec, msghdr, sockaddr_storage};
use nix::sys::socket::{sockaddr, InetAddr, SockAddr};
use nix::sys::uio::IoVec;
use std::mem;
use std::net::UdpSocket;
use std::os::raw::c_void;
use std::os::unix::io::AsRawFd;
use std::ptr;
#[derive(Parser, Debug)]
#[clap(name = "io_uring sender")]
#[clap(author, version, long_about=None)]
struct Opts {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Receive {
        #[clap(default_value_t = 16)]
        batch_size: usize,
    },
    Send {
        #[clap(default_value_t = 16)]
        batch_size: usize,
        #[clap(default_value_t = 512)]
        buffer_size: usize,
    },
}

fn sender(batch_size: usize, msg_size: usize) -> std::io::Result<()> {
    let mut builder = IoUring::builder();
    builder.dontfork();
    let sock = UdpSocket::bind("127.0.0.1:0").expect("Could not bind port. Why? Who knows.");
    let sock_fd = sock.as_raw_fd();
    let mut ioring = builder.build(batch_size as u32)?;
    let buf: Vec<Vec<u8>> = vec![vec![0; msg_size]; batch_size];
    let sockaddr = "127.0.0.1:7200".parse().expect("Could not parse");
    let sock_addr = SockAddr::new_inet(InetAddr::from_std(&sockaddr));
    let (mname, mlen) = sock_addr.as_ffi_pair();
    let mut iovecs: Vec<IoVec<&[u8]>> = buf.iter().map(|buf| IoVec::from_slice(&buf[..])).collect();
    // Clippy is wrong in this case. Replacing `clone()` with
    // Copy will break this code.
    let mut mname = mname.clone();
    let hdrs: Vec<msghdr> = iovecs
        .iter_mut()
        .map(|iovec| msghdr {
            msg_name: &mut mname as *mut sockaddr as *mut c_void,
            msg_namelen: mlen,
            msg_iov: iovec as *mut IoVec<&[u8]> as *mut iovec,
            msg_iovlen: 1,
            msg_flags: 0,
            msg_controllen: 0,
            msg_control: ptr::null_mut(),
        })
        .collect();
    loop {
        for hdr in hdrs.iter() {
            let write_e = opcode::SendMsg::new(types::Fd(sock_fd), hdr as _).build();
            unsafe {
                ioring.submission().push(&write_e).expect("Could not push");
            }
        }
        ioring.submit_and_wait(batch_size)?;
        for cqe in ioring.completion() {
            let result = cqe.result();
            if result < 0 {
                println!("Error: {}", errno::from_i32(-cqe.result()));
            }
        }
    }
}

fn receiver(batch_size: usize) -> std::io::Result<()> {
    let mut builder = IoUring::builder();
    // The 1ms stall timer is sort of arbitrary here.
    builder.dontfork().setup_sqpoll(1);
    let sock =
        UdpSocket::bind("127.0.0.1:7200").expect("Could not bind port 7200. Why? Who knows.");
    let sock_fd = sock.as_raw_fd();
    let mut ioring = builder.build(batch_size as u32)?;
    let buf = vec![vec![0; 2048]; batch_size];
    // sockaddr_storage is guaranteed to be large enough to hold any possible address.
    let mut sockaddrs: Vec<sockaddr_storage> = (0..batch_size)
        .map(|_| unsafe { mem::MaybeUninit::zeroed().assume_init() })
        .collect();
    let mut iovecs: Vec<IoVec<&[u8]>> = buf.iter().map(|buf| IoVec::from_slice(&buf[..])).collect();
    let mut hdrs: Vec<msghdr> = iovecs
        .iter_mut()
        .zip(sockaddrs.iter_mut())
        .map(|(iovec, mname)| msghdr {
            msg_name: mname as *mut sockaddr_storage as *mut c_void,
            msg_namelen: mem::size_of::<sockaddr_storage>() as u32,
            msg_iov: iovec as *mut IoVec<&[u8]> as *mut iovec,
            msg_iovlen: 1,
            msg_flags: 0,
            msg_controllen: 0,
            msg_control: ptr::null_mut(),
        })
        .collect();
    let files = [sock_fd];
    ioring
        .submitter()
        .register_files(&files[..])
        .expect("Could not register file");
    let read_ops: Vec<Entry> = hdrs
        .iter_mut()
        .enumerate()
        .map(|(i, hdr)| {
            opcode::RecvMsg::new(types::Fd(sock_fd), hdr as _)
                .build()
                .user_data(i as u64)
        })
        .collect();
    // Step 1: Set up for initial reception
    for op in read_ops.iter() {
        unsafe {
            ioring.submission().push(op).expect("Could not push");
        }
    }
    'outer: loop {
        // Step 2: When we arrive at this point, the cqueue was either empty
        // (or we just started the receive process). Either ways, kick the tires
        // and wait until we receive something.
        ioring.submit_and_wait(1)?;
        // Step 3: We must have received something so process as much data as we can.
        let mut to_process = true;
        let mut processed = 0;
        let mut bytes = 0;
        while to_process {
            processed += 1;
            let idx = {
                if let Some(cqe) = ioring.completion().next() {
                    let idx = cqe.user_data() as usize;
                    if cqe.result() < 0 {
                        println!("Error: {}", errno::from_i32(-cqe.result()));
                    } else {
                        let len = cqe.result() as usize;
                        let _addr = unsafe {
                            SockAddr::from_libc_sockaddr(
                                &sockaddrs[idx] as *const sockaddr_storage as *const sockaddr,
                            )
                        }
                        .expect("Could not interpret address");
                        bytes += len
                    }
                    idx
                } else {
                    continue 'outer;
                }
            };
            unsafe {
                while ioring.submission().push(&read_ops[idx]).is_err() {
                    ioring.submit_and_wait(1)?;
                }
            }
            to_process = if ioring.completion().is_empty() {
                ioring.completion().sync();
                !ioring.completion().is_empty()
            } else {
                true
            }
        }
        println!("Processed {processed} reqs ({bytes} bytes) before waiting");
    }
}

fn main() -> std::io::Result<()> {
    match Opts::parse().command {
        Command::Receive { batch_size } => receiver(batch_size),
        Command::Send {
            batch_size,
            buffer_size,
        } => sender(batch_size, buffer_size),
    }
}

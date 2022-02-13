use clap::{Parser, Subcommand};
use io_uring::{opcode, types, IoUring};
use nix::libc::iovec;
use nix::libc::msghdr;
use nix::sys::socket::{sockaddr, InetAddr, SockAddr};
use nix::sys::uio::IoVec;
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
    let mut ioring = builder.build(32)?;
    let buf: Vec<Vec<u8>> = vec![vec![0; msg_size]; batch_size];
    let sockaddr = "127.0.0.1:7200".parse().expect("Could not parse");
    let sock_addr = SockAddr::new_inet(InetAddr::from_std(&sockaddr));
    let (mname, mlen) = sock_addr.as_ffi_pair();
    let hdrs: Vec<msghdr> = (0..batch_size)
        .map(|i| {
            let mut mname = mname.clone();
            msghdr {
                msg_name: &mut mname as *mut sockaddr as *mut c_void,
                msg_namelen: mlen,
                msg_iov: &mut [IoVec::from_slice(&buf[i][..])] as *mut IoVec<&[u8]> as *mut iovec,
                msg_iovlen: 1,
                msg_flags: 0,
                msg_controllen: 0,
                msg_control: ptr::null_mut(),
            }
        })
        .collect();
    // loop {
    for hdr in hdrs.iter() {
        let write_e = opcode::SendMsg::new(types::Fd(sock_fd), hdr as _).build();
        unsafe {
            ioring.submission().push(&write_e).expect("Could not push");
        }
    }
    ioring.submit_and_wait(batch_size)?;
    for cqe in ioring.completion() {
        let result = cqe.result() as usize;
        println!("result was {result}");
    }
    // }
    Ok(())
}

fn receiver(batch_size: usize) -> std::io::Result<()> {
    let mut builder = IoUring::builder();
    builder.dontfork();
    let sock =
        UdpSocket::bind("127.0.0.1:7200").expect("Could not bind port 7200. Why? Who knows.");
    let sock_fd = sock.as_raw_fd();
    let mut ioring = builder.build(32)?;
    let mut buf = vec![vec![0; 2048]; batch_size];
    loop {
        for buf in buf.iter_mut() {
            let read_e =
                opcode::Read::new(types::Fd(sock_fd), buf.as_mut_ptr(), buf.len() as _).build();
            unsafe {
                ioring.submission().push(&read_e).expect("Could not push");
            }
        }
        ioring.submit_and_wait(batch_size)?;
        for (i, buf) in buf.iter().enumerate() {
            let cqe = ioring
                .completion()
                .next()
                .expect("Completion queue is empty");
            let result = cqe.result() as usize;
            println!(
                "{i}th result was {}",
                std::str::from_utf8(&(buf[0..result])).expect("Could not parse")
            );
        }
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

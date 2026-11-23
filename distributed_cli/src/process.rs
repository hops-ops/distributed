//! Child processes owned by a `build` or `dev` session.
//!
//! A session starts each child as the leader of its own process group and owns
//! it through a [`ProcessGroup`]. Dropping the owner kills the whole group, so
//! a session that ends for any reason (a signal, a cancelled build, an error
//! return, a panic) leaves no descendant behind.
//!
//! Only the session's own process creates groups. A helper it runs (`describe`,
//! `__js-watch`) starts its children in the group it was given: a nested group
//! would escape the session's kill. So [`run`] and [`output`] take the session's
//! stop flag when the session calls them and `None` everywhere else.
//!
//! A group is signalled by its leader's process ID. Once the leader has been
//! reaped and the last member has exited, that ID is free again, so nothing
//! here signals a group it has already seen empty.
//!
//! Windows has no process groups. There the owner controls the direct child
//! only, and a grandchild can outlive the session.

use std::io::{self, Read};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const POLL: Duration = Duration::from_millis(10);
/// How long a foreground command gets to exit after SIGTERM.
const GRACE: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) struct ProcessGroup {
    leader: Child,
    /// The leader was reaped and no member was left.
    gone: bool,
}

impl ProcessGroup {
    /// Start `command` as the leader of a new process group.
    pub(crate) fn spawn(command: &mut Command) -> io::Result<Self> {
        #[cfg(unix)]
        std::os::unix::process::CommandExt::process_group(command, 0);
        Ok(Self {
            leader: command.spawn()?,
            gone: false,
        })
    }

    /// The direct child: its pipes, and its exit status once it has one.
    pub(crate) fn leader(&mut self) -> &mut Child {
        &mut self.leader
    }

    /// Whether the leader or any other member of its group is still alive.
    pub(crate) fn is_running(&mut self) -> io::Result<bool> {
        if self.gone {
            return Ok(false);
        }
        let mut running = self.leader.try_wait()?.is_none();
        #[cfg(unix)]
        if !running {
            running = match self.signal(0) {
                Ok(delivered) => delivered,
                // A member this user may not signal is still a member.
                Err(error) if error.raw_os_error() == Some(libc::EPERM) => true,
                Err(error) => return Err(error),
            };
        }
        self.gone = !running;
        Ok(running)
    }

    /// Ask every member to exit (SIGTERM).
    pub(crate) fn terminate(&mut self) -> io::Result<()> {
        if !self.is_running()? {
            return Ok(());
        }
        #[cfg(unix)]
        {
            self.signal(libc::SIGTERM).map(drop)
        }
        #[cfg(not(unix))]
        {
            self.leader.kill()
        }
    }

    /// Kill every member (SIGKILL) and reap the leader.
    ///
    /// SIGKILL cannot be caught and reaches every member in one call, so none
    /// survives it. This does not wait for the kernel to finish removing
    /// them: how long that takes depends on the machine's load, not on them.
    pub(crate) fn kill(&mut self) -> io::Result<()> {
        if self.gone {
            return Ok(());
        }
        #[cfg(unix)]
        match self.signal(libc::SIGKILL) {
            Ok(_) => {}
            // macOS answers EPERM for a group whose members are all exiting.
            Err(error) if error.raw_os_error() == Some(libc::EPERM) => {}
            Err(error) => return Err(error),
        }
        // A leader that left its own group is not reached by the group signal.
        // `Child::kill` does nothing once the leader has been reaped.
        self.leader.kill()?;
        self.leader.wait()?;
        self.gone = true;
        Ok(())
    }

    /// Wait up to `timeout` for the whole group to exit.
    pub(crate) fn wait_gone(&mut self, timeout: Duration) -> io::Result<bool> {
        let started = Instant::now();
        loop {
            if !self.is_running()? {
                return Ok(true);
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            std::thread::sleep(POLL);
        }
    }

    /// Ask the group to exit, then kill whatever is left after `grace`.
    pub(crate) fn stop(&mut self, grace: Duration) -> io::Result<()> {
        self.terminate()?;
        if self.wait_gone(grace)? {
            return Ok(());
        }
        self.kill()
    }

    /// Send `signal` to the group. `Ok(false)` means the group has no member.
    #[cfg(unix)]
    fn signal(&self, signal: libc::c_int) -> io::Result<bool> {
        // SAFETY: `kill` takes plain integers and touches no memory.
        if unsafe { libc::kill(-(self.leader.id() as libc::pid_t), signal) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

/// Run `command` until it exits or the session stops.
///
/// `stop` is the session's stop flag: the command then leads its own process
/// group, and `None` is returned when it was stopped, its group gone. It gets
/// no standard input, because a background process group that reads the
/// terminal is suspended.
///
/// Without `stop` this process is not a session (it is a helper inside a
/// session's group, or a one-off command) and the command is its ordinary
/// child.
pub(crate) fn run(
    command: &mut Command,
    stop: Option<&AtomicBool>,
) -> io::Result<Option<ExitStatus>> {
    let Some(stop) = stop else {
        return command.status().map(Some);
    };
    if stop.load(Ordering::SeqCst) {
        return Ok(None);
    }
    let mut group = ProcessGroup::spawn(command.stdin(Stdio::null()))?;
    wait(&mut group, stop)
}

/// [`run`], collecting what the command writes to its output and error streams.
pub(crate) fn output(
    command: &mut Command,
    stop: Option<&AtomicBool>,
) -> io::Result<Option<Output>> {
    let Some(stop) = stop else {
        return command.output().map(Some);
    };
    if stop.load(Ordering::SeqCst) {
        return Ok(None);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut group = ProcessGroup::spawn(command)?;
    let stdout = drain(group.leader().stdout.take());
    let stderr = drain(group.leader().stderr.take());
    let status = wait(&mut group, stop)?;
    // A member that outlived the leader still holds the pipes open.
    drop(group);
    let (stdout, stderr) = (stdout.join(), stderr.join());
    let collected = |stream: std::thread::Result<io::Result<Vec<u8>>>| {
        stream.unwrap_or_else(|_| Err(io::Error::other("output reader panicked")))
    };
    let (stdout, stderr) = (collected(stdout)?, collected(stderr)?);
    Ok(status.map(|status| Output {
        status,
        stdout,
        stderr,
    }))
}

/// Wait for the leader to exit; stop the whole group when `stop` is set first.
fn wait(group: &mut ProcessGroup, stop: &AtomicBool) -> io::Result<Option<ExitStatus>> {
    loop {
        if let Some(status) = group.leader().try_wait()? {
            return Ok(Some(status));
        }
        if stop.load(Ordering::SeqCst) {
            group.stop(GRACE)?;
            return Ok(None);
        }
        std::thread::sleep(POLL);
    }
}

/// Read a pipe to its end on another thread, so a full pipe never blocks the
/// command.
fn drain(pipe: Option<impl Read + Send + 'static>) -> std::thread::JoinHandle<io::Result<Vec<u8>>> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        if let Some(mut pipe) = pipe {
            pipe.read_to_end(&mut bytes)?;
        }
        Ok(bytes)
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};

    /// A leader that ignores SIGTERM and starts a grandchild that does too,
    /// then prints both process IDs.
    fn stubborn_family() -> (ProcessGroup, [libc::pid_t; 2]) {
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                r#"trap "" TERM
/bin/sh -c 'trap "" TERM; while :; do sleep 1; done' &
echo "$$ $!"
wait"#,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped());
        let mut group = ProcessGroup::spawn(&mut command).expect("spawn process group");
        let mut line = String::new();
        BufReader::new(group.leader().stdout.take().expect("leader stdout"))
            .read_line(&mut line)
            .expect("read family process IDs");
        let mut pids = line
            .split_whitespace()
            .map(|pid| pid.parse().expect("process ID"));
        let family = [pids.next().unwrap(), pids.next().unwrap()];
        assert_eq!(family[0], group.leader().id() as libc::pid_t);
        (group, family)
    }

    /// A process is gone once it cannot be signalled. The grandchild is not
    /// this test's child, so nothing here can leave it a zombie.
    fn assert_gone(family: [libc::pid_t; 2]) {
        let started = Instant::now();
        loop {
            let alive = family
                .into_iter()
                .filter(|pid| unsafe { libc::kill(*pid, 0) } == 0)
                .collect::<Vec<_>>();
            if alive.is_empty() {
                return;
            }
            if started.elapsed() > Duration::from_secs(10) {
                for pid in &alive {
                    unsafe { libc::kill(*pid, libc::SIGKILL) };
                }
                panic!("processes outlived their group owner: {alive:?}");
            }
            std::thread::sleep(POLL);
        }
    }

    #[test]
    fn dropping_the_owner_kills_the_whole_group() {
        let (group, family) = stubborn_family();
        drop(group);
        assert_gone(family);
    }

    #[test]
    fn a_panicking_owner_kills_the_whole_group() {
        let (group, family) = stubborn_family();
        let owner = std::thread::spawn(move || {
            let _group = group;
            panic!("the session failed");
        });
        assert!(owner.join().is_err());
        assert_gone(family);
    }

    #[test]
    fn stop_kills_members_that_ignore_the_request() {
        let (mut group, family) = stubborn_family();
        group.stop(Duration::from_millis(50)).expect("stop group");
        assert!(!group.is_running().unwrap());
        assert_gone(family);
    }

    #[test]
    fn a_group_outlives_its_leader_until_killed() {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "sleep 60 & echo $!"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped());
        let mut group = ProcessGroup::spawn(&mut command).expect("spawn process group");
        let mut line = String::new();
        BufReader::new(group.leader().stdout.take().expect("leader stdout"))
            .read_line(&mut line)
            .expect("read descendant process ID");
        let descendant: libc::pid_t = line.trim().parse().expect("process ID");
        let leader = group.leader().id() as libc::pid_t;
        assert!(group.leader().wait().expect("wait for leader").success());
        assert!(group.is_running().unwrap(), "the descendant is a member");
        group.kill().expect("kill group");
        assert_gone([leader, descendant]);
    }

    #[test]
    fn a_stopped_command_takes_its_descendants_with_it() {
        let directory = tempfile::tempdir().unwrap();
        let pids = directory.path().join("pids");
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let runner = {
            let (stop, pids) = (std::sync::Arc::clone(&stop), pids.clone());
            std::thread::spawn(move || {
                let mut command = Command::new("/bin/sh");
                command.args([
                    "-c",
                    r#"sleep 60 & echo "$$ $!" > "$0.tmp" && mv "$0.tmp" "$0"; wait"#,
                    pids.to_str().unwrap(),
                ]);
                run(&mut command, Some(&stop))
            })
        };
        let started = Instant::now();
        let family = loop {
            if let Ok(line) = std::fs::read_to_string(&pids) {
                let mut pids = line.split_whitespace().map(|pid| pid.parse().unwrap());
                break [pids.next().unwrap(), pids.next().unwrap()];
            }
            assert!(started.elapsed() < Duration::from_secs(10));
            std::thread::sleep(POLL);
        };
        stop.store(true, Ordering::SeqCst);
        assert!(runner.join().unwrap().unwrap().is_none());
        assert_gone(family);

        let mut ignored = Command::new("/bin/sh");
        ignored.args(["-c", "exit 0"]);
        assert!(run(&mut ignored, Some(&stop)).unwrap().is_none());
        stop.store(false, Ordering::SeqCst);
        assert!(run(&mut ignored, Some(&stop)).unwrap().unwrap().success());
    }

    #[test]
    fn output_is_collected_and_a_stopped_command_takes_its_descendants_with_it() {
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        // More than a pipe holds, on both streams, and a descendant that keeps
        // them open after the leader exits.
        let mut noisy = Command::new("/bin/sh");
        noisy.args([
            "-c",
            "head -c 200000 /dev/zero; head -c 200000 /dev/zero >&2; sleep 60 & exit 3",
        ]);
        let output = output(&mut noisy, Some(&stop)).unwrap().unwrap();
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(
            (output.stdout.len(), output.stderr.len()),
            (200_000, 200_000)
        );

        let directory = tempfile::tempdir().unwrap();
        let pids = directory.path().join("pids");
        let runner = {
            let (stop, pids) = (std::sync::Arc::clone(&stop), pids.clone());
            std::thread::spawn(move || {
                let mut command = Command::new("/bin/sh");
                command.args([
                    "-c",
                    r#"sleep 60 & echo "$$ $!" > "$0.tmp" && mv "$0.tmp" "$0"; wait"#,
                    pids.to_str().unwrap(),
                ]);
                super::output(&mut command, Some(&stop))
            })
        };
        let started = Instant::now();
        let family = loop {
            if let Ok(line) = std::fs::read_to_string(&pids) {
                let mut pids = line.split_whitespace().map(|pid| pid.parse().unwrap());
                break [pids.next().unwrap(), pids.next().unwrap()];
            }
            assert!(started.elapsed() < Duration::from_secs(10));
            std::thread::sleep(POLL);
        };
        stop.store(true, Ordering::SeqCst);
        assert!(runner.join().unwrap().unwrap().is_none());
        assert_gone(family);
    }

    #[test]
    fn outside_a_session_a_command_is_an_ordinary_child() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "echo out; echo err >&2; exit 4"]);
        let output = output(&mut command, None).unwrap().unwrap();
        assert_eq!(output.status.code(), Some(4));
        assert_eq!(
            (output.stdout, output.stderr),
            (b"out\n".to_vec(), b"err\n".to_vec())
        );
        let mut command = Command::new("/bin/sh");
        // Its own process group would have its own ID.
        command.args(["-c", r#"test "$(ps -o pgid= -p $$ | tr -d " ")" != "$$""#]);
        assert!(run(&mut command, None).unwrap().unwrap().success());
    }
}

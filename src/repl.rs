use std::io::{self, Write};

use crate::engine::Engine;
use anyhow::Result;
use log::error;

pub struct Repl {
    engine: Option<Engine>,
}

impl Repl {
    pub fn new() -> Self {
        Self { engine: None }
    }

    fn open(&mut self, name: &str) -> Result<()> {
        self.engine = Some(Engine::new(name)?);
        Ok(())
    }

    fn process_cmd(&mut self, cmd: &str, args: &[&str]) {
        let engine = self.engine.as_mut().unwrap();
        match cmd {
            "set" => engine.set(args[0].as_bytes(), args[1].as_bytes()),
            "get" => {
                if let Some(v) = engine.get(args[0].as_bytes()) {
                    println!("{}", String::from_utf8_lossy(&v));
                } else {
                    println!("no value found");
                }
            }
            "del" => engine.del(args[0].as_bytes()),
            "dump" => {
                for log in &mut engine.logs {
                    println!("{:?}:", log.name);
                    log.dump().unwrap();
                }
            }
            _ => {}
        }
    }

    fn process_line(&mut self, line: &[&str]) {
        match line[0] {
            "open" => {
                let _ = self.open(line[1]).map_err(|e| println!("{}", e));
            }
            cmd => {
                if self.engine.is_none() {
                    println!("open log file first");
                    return;
                }
                self.process_cmd(cmd, &line[1..]);
            }
        }
    }

    pub fn run(&mut self) {
        let mut line = "".to_string();
        loop {
            print!("> ");
            let _ = io::stdout().flush();

            line.clear();
            match io::stdin().read_line(&mut line) {
                Err(e) => {
                    error!("{}", e);
                    continue;
                }
                Ok(_) => {
                    let line: Vec<&str> = line.split_whitespace().collect();
                    self.process_line(&line);
                }
            }
        }
    }
}

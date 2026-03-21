use std::io::{self, Write};

use crate::engine::Engine;
use anyhow::Result;
use log::error;

pub struct Repl {
    engine: Engine,
}

impl Repl {
    pub fn new() -> Self {
        Self {
            engine: Engine::new(),
        }
    }

    fn open(&mut self, name: &str) -> Result<()> {
        self.engine.open_log_dir(name)?;
        Ok(())
    }

    fn process_cmd(&mut self, cmd: &str, args: &[&str]) {
        match cmd {
            "set" => {
                if args.len() != 2 {
                    println!("expect a key and a value");
                    return;
                }
                self.engine.set(args[0].to_string(), args[1].to_string());
            }
            "get" => {
                if args.len() != 1 {
                    println!("expect a key");
                    return;
                }
                if let Ok(v) = self.engine.get(args[0]) {
                    println!("{}", v);
                } else {
                    println!("no value found");
                }
            }
            "del" => {
                if args.len() != 1 {
                    println!("expect a key");
                    return;
                }
                self.engine.del(args[0]);
            }
            "dump" => {
                self.engine.dump();
            }
            _ => {}
        }
    }

    fn process_line(&mut self, line: &[&str]) {
        if line.is_empty() {
            return;
        }
        match line[0] {
            "open" => {
                if line.len() != 2 {
                    println!("expect a path to a directory");
                    return;
                }
                let _ = self.open(line[1]).map_err(|e| println!("{}", e));
            }
            cmd => {
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

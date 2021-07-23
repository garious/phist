use clap::{crate_version, App, Arg};
use serde::{Deserialize, Serialize};
use serde_json::Result;
use solana_bpf_loader_program::{
    create_vm, serialization::serialize_parameters, syscalls::register_syscalls, BpfError,
    ThisInstructionMeter,
};
use solana_rbpf::{
    assembler::assemble,
    static_analysis::Analysis,
    verifier::check,
    vm::{Config, DynamicAnalysis, Executable},
};
use solana_sdk::{
    account::AccountSharedData,
    bpf_loader,
    keyed_account::KeyedAccount,
    process_instruction::{InvokeContext, MockInvokeContext},
    pubkey::Pubkey,
};
use std::{cell::RefCell, fs::File, io::Read, io::Seek, io::SeekFrom, path::Path};
use time::Instant;

#[derive(Serialize, Deserialize, Debug)]
struct Account {
    lamports: u64,
    data: Vec<u8>,
    owner: Pubkey,
}
#[derive(Serialize, Deserialize)]
struct Input {
    accounts: Vec<Account>,
    insndata: Vec<u8>,
}
fn load_accounts(path: &Path) -> Result<Input> {
    let file = File::open(path).unwrap();
    let input: Input = serde_json::from_reader(file)?;
    println!("Program input:");
    println!("accounts {:?}", &input.accounts);
    println!("insndata {:?}", &input.insndata);
    println!("----------------------------------------");
    Ok(input)
}

fn main() {
    solana_logger::setup();
    let matches = App::new("Solana BPF CLI")
        .version(crate_version!())
        .author("Solana Maintainers <maintainers@solana.foundation>")
        .about(
            r##"CLI to test and analyze eBPF programs.

The tool executes eBPF programs in a mocked environment.
Some features, such as sysvars syscall and CPI, are not
available for the programs executed by the CLI tool.

The input data for a program execution have to be in JSON format
and the following fields are required
{
    "accounts": [
        {
            "lamports": 1000,
            "data": [0, 0, 0, 3],
            "owner": [
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        }
    ],
    "insndata": []
}
"##,
        )
        .arg(
            Arg::new("PROGRAM")
                .about(
                    "Program file to use. This is either an ELF shared-object file to be executed, \
                     or an assembly file to be assembled and executed.",
                )
                .required(true)
                .index(1)
        )
        .arg(
            Arg::new("input")
                .about(
                    "Input for the program to run on, where FILE is a name of a JSON file \
with input data, or BYTES is the number of 0-valued bytes to allocate for program parameters",
                )
                .short('i')
                .long("input")
                .value_name("FILE / BYTES")
                .takes_value(true)
                .default_value("0"),
        )
        .arg(
            Arg::new("memory")
                .about("Heap memory for the program to run on")
                .short('m')
                .long("memory")
                .value_name("BYTES")
                .takes_value(true)
                .default_value("0"),
        )
        .arg(
            Arg::new("use")
                .about(
                    "Method of execution to use, where 'cfg' generates Control Flow Graph \
of the program, 'disassembler' dumps disassembled code of the program, 'interpreter' runs \
the program in the virtual machine's interpreter, and 'jit' precompiles the program to \
native machine code before execting it in the virtual machine.",
                )
                .short('u')
                .long("use")
                .takes_value(true)
                .value_name("VALUE")
                .possible_values(&["cfg", "disassembler", "interpreter", "jit"])
                .default_value("jit"),
        )
        .arg(
            Arg::new("instruction limit")
                .about("Limit the number of instructions to execute")
                .short('l')
                .long("limit")
                .takes_value(true)
                .value_name("COUNT")
                .default_value(&std::i64::MAX.to_string()),
        )
        .arg(
            Arg::new("trace")
                .about("Output trace to 'trace.out' file using tracing instrumentation")
                .short('t')
                .long("trace"),
        )
        .arg(
            Arg::new("profile")
                .about("Output profile to 'profile.dot' file using tracing instrumentation")
                .short('p')
                .long("profile"),
        )
        .arg(
            Arg::new("verify")
                .about("Run the verifier before execution or disassembly")
                .short('v')
                .long("verify"),
        )
        .get_matches();

    let config = Config {
        enable_instruction_tracing: matches.is_present("trace") || matches.is_present("profile"),
        ..Config::default()
    };
    let mut accounts = Vec::new();
    let mut account_refcells = Vec::new();
    let default_account = RefCell::new(AccountSharedData::default());
    let key = solana_sdk::pubkey::new_rand();
    let mut mem = match matches.value_of("input").unwrap().parse::<usize>() {
        Ok(allocate) => {
            accounts.push(KeyedAccount::new(&key, false, &default_account));
            vec![0u8; allocate]
        }
        Err(_) => {
            let input = load_accounts(Path::new(matches.value_of("input").unwrap())).unwrap();
            for acc in input.accounts {
                let asd = AccountSharedData::new_ref(acc.lamports, acc.data.len(), &acc.owner);
                asd.borrow_mut().set_data(acc.data);
                account_refcells.push(asd);
            }
            for acc in &account_refcells {
                accounts.push(KeyedAccount::new(&key, false, acc));
            }
            let lid = bpf_loader::id();
            let pid = Pubkey::new(&[0u8; 32]);
            let mut bytes = serialize_parameters(&lid, &pid, &accounts, &input.insndata).unwrap();
            Vec::from(bytes.as_slice_mut())
        }
    };
    let mut invoke_context = MockInvokeContext::new(accounts);
    let logger = invoke_context.logger.clone();
    let compute_meter = invoke_context.get_compute_meter();
    let mut instruction_meter = ThisInstructionMeter { compute_meter };

    let program = matches.value_of("PROGRAM").unwrap();
    let mut file = File::open(&Path::new(program)).unwrap();
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    let syscall_registry = register_syscalls(&mut invoke_context).unwrap();
    let mut executable = if magic == [0x7f, 0x45, 0x4c, 0x46] {
        <dyn Executable<BpfError, ThisInstructionMeter>>::from_elf(
            &contents,
            None,
            config,
            syscall_registry,
        )
        .map_err(|err| format!("Executable constructor failed: {:?}", err))
    } else {
        assemble::<BpfError, ThisInstructionMeter>(
            std::str::from_utf8(contents.as_slice()).unwrap(),
            None,
            config,
            syscall_registry,
        )
    }
    .unwrap();

    if matches.is_present("verify") {
        let text_bytes = executable.get_text_bytes().1;
        check(text_bytes, &config).unwrap();
    }
    executable.jit_compile().unwrap();
    let analysis = Analysis::from_executable(executable.as_ref());

    match matches.value_of("use") {
        Some("cfg") => {
            let mut file = File::create("cfg.dot").unwrap();
            analysis.visualize_graphically(&mut file, None).unwrap();
            return;
        }
        Some("disassembler") => {
            let stdout = std::io::stdout();
            analysis.disassemble(&mut stdout.lock()).unwrap();
            return;
        }
        _ => {}
    }

    let id = bpf_loader::id();
    let mut vm = create_vm(&id, executable.as_ref(), &mut mem, &mut invoke_context).unwrap();
    let start_time = Instant::now();
    let result = if matches.value_of("use").unwrap() == "interpreter" {
        vm.execute_program_interpreted(&mut instruction_meter)
    } else {
        vm.execute_program_jit(&mut instruction_meter)
    };
    let duration = Instant::now() - start_time;
    if logger.log.borrow().len() > 0 {
        println!("Program output:");
        for s in logger.log.borrow_mut().iter() {
            println!("{}", s);
        }
        println!("----------------------------------------");
    }
    println!("Result: {:?}", result);
    println!("Instruction Count: {}", vm.get_total_instruction_count());
    println!("Execution time: {} us", duration.whole_microseconds());
    if matches.is_present("trace") {
        println!("Trace is saved in trace.out");
        let mut file = File::create("trace.out").unwrap();
        let analysis = Analysis::from_executable(executable.as_ref());
        vm.get_tracer().write(&mut file, &analysis).unwrap();
    }
    if matches.is_present("profile") {
        println!("Profile is saved in profile.dot");
        let tracer = &vm.get_tracer();
        let dynamic_analysis = DynamicAnalysis::new(tracer, &analysis);
        let mut file = File::create("profile.dot").unwrap();
        analysis
            .visualize_graphically(&mut file, Some(&dynamic_analysis))
            .unwrap();
    }
}

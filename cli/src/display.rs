use crate::cli::SettingType;
use console::style;
use solana_sdk::hash::Hash;

// Pretty print a "name value"
pub fn println_name_value(name: &str, value: &str) {
    let styled_value = if value == "" {
        style("(not set)").italic()
    } else {
        style(value)
    };
    println!("{} {}", style(name).bold(), styled_value);
}

pub fn println_name_value_or(name: &str, value: &str, setting_type: SettingType) {
    let description = match setting_type {
        SettingType::Explicit => "",
        SettingType::Computed => "(computed)",
        SettingType::SystemDefault => "(default)",
    };

    println!(
        "{} {} {}",
        style(name).bold(),
        style(value),
        style(description).italic(),
    );
}

pub fn println_signers(blockhash: &Hash, signers: &[String]) {
    println!();
    println!("Blockhash: {}", blockhash);
    if !signers.is_empty() {
        println!("Signers (Pubkey=Signature):");
        signers.iter().for_each(|signer| println!("  {}", signer))
    }
    println!();
}

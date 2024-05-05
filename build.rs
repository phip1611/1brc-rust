use std::fs;
use std::path::Path;

// Keep in sync with .gitignore file!
const TESTDATA_PATH: &str = "./measurements.txt";
const ROWS_TO_GENERATE: usize = 1_000_000_000;

fn main() {
    if !Path::new(TESTDATA_PATH).exists() {
        checkout_submodules();
        build_maven_project();
        generate_testdata();
    }
}

fn checkout_submodules() {
    std::process::Command::new("git")
        .args(["submodule", "update", "--init"])
        .output()
        .expect("Failed to fetch git submodules!");
}

fn build_maven_project() {
    std::process::Command::new("mvn")
        .args(["package", "-T", "2C"])
        .current_dir("./1brc")
        .output()
        .expect("Failed to fetch  submodules!");
}

fn generate_testdata() {
    let _ = fs::remove_file("./1brc/measurements.txt");

    std::process::Command::new("./create_measurements.sh")
        .args([format!("{ROWS_TO_GENERATE}")])
        .current_dir("./1brc")
        .output()
        .expect("Failed to create measurements.txt!");

    fs::copy("./1brc/measurements.txt", "./measurements.txt").unwrap();
}

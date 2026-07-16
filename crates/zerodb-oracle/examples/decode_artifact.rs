//! Print the op sequence a fuzz artifact decodes to.
fn main() {
    let path = std::env::args().nth(1).expect("artifact path");
    let bytes = std::fs::read(path).unwrap();
    for (i, op) in zerodb_oracle::decode_ops(&bytes, 64).iter().enumerate() {
        println!("{i:3}: {op:?}");
    }
}

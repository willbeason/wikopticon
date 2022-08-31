fn main() {
    let db = sled::open("abc").expect("unable to open Sled database");

    db.insert(&[1],&[2]).expect("it works");

    println!("Hello, world!");
}

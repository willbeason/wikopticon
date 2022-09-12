mod page;

extern crate xml;

use std::fs::File;
use std::io::BufReader;

use xml::reader::{EventReader, XmlEvent};

fn indent(size: usize) -> String {
    const INDENT: &'static str = "    ";
    (0..size).map(|_| INDENT)
        .fold(String::with_capacity(size*INDENT.len()), |r, s| r + s)
}

fn main() {
    // /c/"Program Files"/MongoDB/Server/6.0/bin/mongod.exe --dbpath="C:\\Users\\Will\\IdeaProjects\\wikopticon\\20220801-1"
    let wikipedia_path = "data\\20220801-01\\enwiki-20220801-pages-articles-multistream1.xml";
    // let wikipedia_date = "20220801-1";

    let file = File::open(wikipedia_path).expect("could not locate wikipedia dump");
    let file = BufReader::new(file);

    let parser = EventReader::new(file);
    let mut depth = 0;

    let mut field = "".to_string();
    let mut cur_page = page::Page::new();

    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, .. }) => {
                if name.local_name == "page" {
                    cur_page = page::Page::new();
                } else {
                    field = name.local_name;
                }
            }
            Ok(XmlEvent::Characters(str)) => {
                match field.as_str() {
                    "title"=> cur_page.title = str,
                    "ns"=> cur_page.ns = str.parse::<u8>().expect("unable to parse namespace"),
                    "id"=>cur_page.id = str.parse::<u64>().expect("unable to parse ID"),
                    "text"=>cur_page.text = str,
                    _=>{}
                }
            }
            Ok(XmlEvent::EndElement { name }) => {
                if name.local_name == "page" {
                    println!("{}", cur_page.id);
                    println!("{}", cur_page.title);
                    println!("{}", cur_page.ns);
                    println!("{}", cur_page.text);
                    break;
                }
            }
            Err(e) => {
                println!("Error: {}", e);
                break;
            }

            _ => {}
        }
    }

    println!("Hello, world!");
}

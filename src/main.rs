extern crate xml;

use std::fs::File;
use std::io::BufReader;
use std::error::Error;
use mongodb::{Client, options::ClientOptions};
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb::options::ReplaceOptions;
use serde::{Deserialize, Serialize};

use xml::reader::{EventReader, XmlEvent};

#[derive(Debug, Serialize, Deserialize)]
struct Article {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<ObjectId>,
    title: String,
    namespace: String,
    redirect: Option<String>,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;

    let client: Client = Client::with_options(client_options).expect("unable to connect to MongoDB");

    // println!("Databases:");
    // for name in client.list_database_names(None, None).await? {
    //     println!("- {}", name);
    // }
    // return Ok(());

    // /c/"Program Files"/MongoDB/Server/6.0/bin/mongod.exe --dbpath="C:\\Users\\Will\\IdeaProjects\\wikopticon\\20220801-1"
    let wikipedia_path = "data\\20220801-01\\enwiki-20220801-pages-articles-multistream1.xml";
    // let wikipedia_date = "20220801-1";

    let file = File::open(wikipedia_path).expect("could not locate wikipedia dump");
    let file = BufReader::new(file);

    let parser = EventReader::new(file);

    let mut field = "".to_string();
    let mut cur_page = Article{
        id: None,
        title: "".to_string(),
        namespace: "".to_string(),
        redirect: None,
        text: "".to_string(),
    };

    let pages: mongodb::Collection<Article> = client.database("wikipedia").collection("pages");

    let mut n = 0;

    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                if name.local_name == "page" {
                    cur_page = Article {
                        id: None,
                        title: "".to_string(),
                        namespace: "".to_string(),
                        redirect: None,
                        text: "".to_string(),
                    };
                } else {
                    field = name.local_name;
                    if field == "redirect" {
                        cur_page.redirect = attributes.into_iter().find(|a| a.name.local_name == "title").map(|a| a.value);
                    }
                }
            }

            Ok(XmlEvent::Characters(str)) => {
                match field.as_str() {
                    "id" => {
                        let p: [u8;8] = str.parse::<i64>().expect("unable to parse ID").to_ne_bytes();
                        let id = [0, 0, 0, 0, p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]];
                        cur_page.id = Some(ObjectId::from_bytes(id) )
                    },
                    "title" => { cur_page.title = str.to_string() },
                    "ns" => { cur_page.namespace = str.to_string() },
                    "text" => {
                        cur_page.text = str.to_string();
                        {}
                    },
                    "sitename" | "dbname" | "base" | "generator" | "case" | "namespace" | "parentid" | "timestamp" | "username" | "comment" | "model" | "format" | "sha1" | "ip" => {}
                    s => { panic!("unknown field: {}", s) }
                }
            }

            Ok(XmlEvent::EndElement { name }) => {
                if name.local_name == "page" {
                    match cur_page.redirect {
                        Some(_) => continue,
                        None => {},
                    }

                    let id = cur_page.id.expect("required id").to_string();
                    let title = cur_page.title.to_string();

                    // cur_pages.push(cur_page);


                    println!("{}: {}", id, title);

                    let mut options = ReplaceOptions::default();
                    options.upsert = Some(true);

                    pages.replace_one(doc!{"id": cur_page.id},
                                     cur_page, options).await
                        .expect("unable to write to database");
                    n = n+1;
                    if n >= 20 {
                        break
                    }

                    cur_page = Article {
                        id: None,
                        title: "".to_string(),
                        namespace: "".to_string(),
                        redirect: None,
                        text: "".to_string(),
                    };
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

    return Ok(());
}

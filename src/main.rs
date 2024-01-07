use std::collections::hash_map::DefaultHasher;
use std::env;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::ops::Add;
use std::os::windows::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::string::ToString;
use std::thread::sleep;
use std::time::Duration;
use chrono::{Datelike, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use num::Float;
use redis::{Commands, Connection};

fn main() {
    clean_start();
}

fn clean_start() {
    // 获取当前工作目录
    let str = env::current_dir().unwrap().display().to_string();
    let path = Path::new(&str);
    let root_path = get_root_path(&path);
    let root = Path::new(&root_path);
    let host_ip = get_local_ip().unwrap();
    println!("{}", root.display().to_string());
    let client = redis::Client::open("redis://docker").unwrap();
    let mut con = client.get_connection().unwrap();
    //对该工作目录所有文件进行整理。
    clean_up(root, &host_ip, &mut con);
//     开启redis 监听文件，
    count_loop(host_ip);
}

fn get_root_path(path: &Path) -> String {
    if path.is_absolute() {
        match path.parent() {
            None => {
                return path.display().to_string();
            }
            Some(parent) => {
                return get_root_path(parent);
            }
        };
    }
    return "/".to_string();
}

fn get_sha256_id(pathes: &Vec<&str>) -> String {
    let mut text = String::new();
    for path in pathes {
        text.push_str(path)
    }
    println!("{}", text);
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    let hash_result = hasher.finish();
    let hash_hex = hash_result.to_be();
    println!("SHA256 hash of '{}': {}", text, hash_hex);
    hash_result.to_string()
}


fn count_loop(host_dirs: String) {
//     自旋 loop
    let host_ip = get_local_ip().unwrap();
    let client = redis::Client::open("redis://docker").unwrap();
    let mut con = client.get_connection().unwrap();
    let mut count_times = 25;
    loop {
        let dirs: String = con.get(&host_dirs).unwrap();
        for dir in dirs.split(",") {
            let key = format!("{host_ip}:{dir}");
            let files: String = con.get(&key).unwrap();
            let files = files.split(",");
            if files.count() < 10 {
                sleep(Duration::from_secs(5));
                break;
            }
            let files: String = con.getset(&key, "").unwrap(); //读写分离
            let mut pathes: Vec<&str> = vec![];
            for (index, file) in files.split(",").enumerate() {
                pathes[index] = file;
            }
            pathes.iter().for_each(|f| println!("{}", f));
            let sha256 = get_sha256_id(&pathes);
            tar_multi_files(&pathes, sha256);
            sleep(Duration::from_secs(5));
        }
        count_times -= 1;
        if count_times < 0 {
            break;
        }
    }
}

fn clean_up(root: &Path, host_ip: &str, con: &mut Connection) {
    let host_dirs = String::from(host_ip).add(":directories");
    let rd = match fs::read_dir(root) {
        Ok(rd) => { rd }
        Err(_) => { return; }
    };
    for entry in rd {
        let entry = entry.unwrap();
        // entry.metadata()TODO redis 存储磁盘记录
        let path = entry.path(); //path
        let path = path.as_path();
        // 对路径进行操作，例如打印文件名
        if path.is_dir() {
            con.set(&host_dirs, &path.display().to_string()).unwrap_or(0);
            clean_up(&Path::new(&String::from(path.display().to_string())), host_ip, con);
        } else if path.is_file() {
            println!("{}", path.display().to_string());
            if is_sys_file(path) {
                continue;
            } else if can_be_deleted(path) {
                fs::remove_file(path).expect("TODO: panic message");
            } else {//但不可删除
                if path.is_absolute() {
                    match path.metadata() {
                        // 获取file元数据
                        // 数据是否大于 50MB
                        //大于 进行压缩，软连接
                        Ok(meta) => {
                            let bytes = Float::floor(meta.file_size() as f64 / 1024f64) as i32;
                            println!("{}", bytes);
                            if bytes > 50 {
                                let pathes = vec![path.to_str().unwrap()];
                                let sha256 = get_sha256_id(&pathes);
                                println!("{}", sha256);
                                let source = tar_multi_files(&pathes, sha256);
                                let mut parent = Path::new(&source).parent().unwrap().display().to_string();
                                println!("{}", parent);
                                //生成链接
                                let mut name = String::new();
                                for x in pathes {
                                    name.push_str(x);
                                    if name.len() > 10 {
                                        break;
                                    }
                                }
                                parent.push_str(&name);
                                parent.push_str(".link");
                                println!("{}", name);
                                let dest = PathBuf::from(name);
                                let source = Path::new(&source);
                                create_shortcut(&source, &dest);
                            } else if get_gap_days(path) { //如果最近访问时间超出2^4天数
                                let mut parent_path = path.parent().unwrap().display().to_string();
                                let key = parent_path.clone();
                                // hostip:directories  : dir, dir,,
                                let mut len = con.strlen(&host_dirs).unwrap_or(0);
                                if len != 0 {
                                    parent_path = String::from(",").add(&parent_path);
                                } else {
                                    len = 1;
                                }
                                con.setrange(&host_dirs, len - 1, parent_path).unwrap_or(0);
                                //setrange 健壮性
                                //ip:directories  :   files ,  files , ,,,,
                                //  let host_dirs = host_ip.clone().add(":directories");
                                //     let host_files = host_ip.add(":files");
                                let file_key = String::from(&host_dirs).add(&key);
                                let mut child_path = path.display().to_string();
                                let mut len = con.strlen(&file_key).unwrap_or(0);
                                if len != 0 {
                                    child_path = String::from(",").add(&child_path);
                                } else {
                                    len = 1;
                                }
                                con.setrange(&host_dirs, len - 1, child_path).unwrap_or(0);
                            }
                        }
                        Err(_) => {}
                    }
                }
            }
        }
    }
}

fn get_gap_days(path: &Path) -> bool {
    // dir stu.xlsx /t:c
    // let path = PathBuf::from(r"C:\Users\33769\Desktop\stu.xlsx");
    let path = path.display().to_string();

    let mut date = Option::Some(String::from("NONE"));
    let output = Command::new("cmd")
        .arg("/c")
        .arg(format!("dir {path} /t:c"))
        .output()
        .unwrap_or_else(|e| {
            panic!("Failed to execute process: {}", e);
        });
    let stdout = String::from_utf8_lossy(&output.stdout);
    let out = stdout.to_string();
    for (index, value) in out.lines().enumerate() {
        if index == 5 {
            date = Option::Some(String::from(&value[0..10]));
        }
    }
    //2024/01/06
    if date.is_none() {
        false;
    }
    let date = match date {
        None => { "2024/01/07".to_string() }
        Some(time) => {
            println!("{}", time);
            if time.eq("NONE"){
                return false;
            }
            time
        }
    };

    let last_year = (&date[0..4]).parse::<isize>().unwrap();
    let last_month = (&date[5..7]).parse::<isize>().unwrap();
    let last_day = (&date[8..10]).parse::<isize>().unwrap();
    let now_time = Utc::now();
    let year: isize = now_time.year() as isize;
    let month: isize = now_time.month() as isize;
    let day: isize = now_time.day() as isize;
    if year - last_year > 2 {
        return true;
    }
    // 3 6
    //  1 2 3 4 5 6 7 8 9 10 11 12
    if last_month > month {
        return true;
    }
    let month_gap = match year - last_year {
        1 => { 12 - last_month + month }
        0 => { month - last_month }
        _ => { 0 }
    };
    if month_gap > 0 {
        return true;
    }
     day - last_day > 15
}

/*pathes : vec[str,str, str,] , 都有一个共同目录*/
fn tar_multi_files(pathes: &Vec<&str>, sha256: String) -> String {
    let mut parent_path = String::from("");
    if pathes.len() > 0 {
        let child_path = PathBuf::from(pathes[0]);
        parent_path = child_path
            .parent()
            .unwrap().to_str()
            .unwrap().to_string();
        parent_path.push_str("\\");
        println!("{}", &parent_path);
    }
    parent_path.push_str(&sha256);
    let mut sha256 = parent_path;
    sha256.push_str(".tar.gz");
    let tar_gz = File::create(&sha256).unwrap();
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = tar::Builder::new(enc);
    for a in pathes.iter() {
        // let mut  file = fs::File::open(a).unwrap();
        let mut file = fs::File::open(a).unwrap();
        let path_buf = PathBuf::from(a);
        let name = path_buf.file_name().unwrap().to_str().unwrap();
        tar.append_file(name, &mut file).ok();
    }
    tar.finish().ok();
    sha256
}

fn is_sys_file(path: &Path) -> bool {
    let sys_types = vec![".dll", ".sys", ".ini", ".cfg", ".ttf", ".otf", ".json",
                         ".xml", ".db", ".sqllite", ".sql", ".dmp"];
    for x in sys_types {
        let ret = path.ends_with(x);
        if ret {
            return true;
        }
    }
    return false;
}

fn can_be_deleted(path: &Path) -> bool {
    let del_types = vec![".tmp ", ".temp", ".thum", ".bak", ".msi", ".log"
                         , ".err", ".tmp", ".jpg", ".png"];
    for x in del_types {
        let ret = path.ends_with(x);
        if ret {
            return true;
        }
    }
    return false;
}


fn create_shortcut(source_file: &Path, shortcut_file: &Path) {
    let source_path = source_file.display().to_string();
    let shortcut_path = shortcut_file.display().to_string();
    // 执行 CMD 命令
    let output = Command::new("cmd")
        .arg("/c")  // 使用 /c 参数来执行后续的命令
        .arg(format!("mklink {shortcut_path} {source_path}")) // 执行 dir 命令，列出当前目录下的文件和文件夹
        .output()
        .unwrap_or_else(|e| {
            panic!("Failed to execute process: {}", e);
        });
    // 打印命令输出
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("STDOUT: {}", stdout);
}


fn get_local_ip() -> Result<String, Box<dyn std::error::Error>> {
    // 使用网络库来获取本机的IP地址
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("8.8.8.8:80").unwrap();
    let local_ip = socket.local_addr().unwrap().ip();
    Ok(local_ip.to_string())
}





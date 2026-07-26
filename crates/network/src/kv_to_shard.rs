//! Application Layer: 把 `Request` 翻译成 `ShardManager` 调用, 把结果翻译回 `Response`.
//!
//! 这一层不知道 codec 长什么样, 也不接触 IO. 纯业务 dispatch.

use shard_manager::ShardManager;

use crate::protocol::{Request, Response};

pub fn dispatch_request(mgr: &ShardManager, db: &str, table: &str, req: Request) -> Response {
    match req {
        Request::Put { key, value } => match mgr.put(db, table, &key, &value, 0) {
            Ok(()) => Response::PutOk,
            Err(e) => Response::Error(format!("put failed: {e}")),
        },
        Request::Get { key } => match mgr.get(db, table, &key, 0) {
            Ok(Some(v)) => Response::Get(Some(v)),
            Ok(None) => Response::Get(None),
            Err(e) => Response::Error(format!("get failed: {e}")),
        },
        Request::Delete { key } => match mgr.delete(db, table, &key, 0) {
            Ok(_) => Response::DeleteOk,
            Err(e) => Response::Error(format!("delete failed: {e}")),
        },
    }
}
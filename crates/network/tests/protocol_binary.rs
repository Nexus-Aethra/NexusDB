//! BinaryProtocol round-trip tests.

use network::protocol::{DecodeOutcome, Protocol, Request, Response, ProtocolError};
use network::BinaryProtocol;

#[test]
fn put_roundtrip() {
    let p = BinaryProtocol;
    let bytes = p.encode_request(42, &Request::Put {
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
    });
    match p.decode_request(&bytes).unwrap() {
        DecodeOutcome::Complete { consumed, value } => {
            assert_eq!(consumed, bytes.len());
            // ⭐ decode 时 Put.value 预置 1B type tag (0x01 = TAG_RAW)
            assert_eq!(value, Request::Put {
                key: b"hello".to_vec(),
                value: b"\x01world".to_vec(),
            });
        }
        DecodeOutcome::NeedMore => panic!("should be complete"),
    }
}

#[test]
fn get_roundtrip() {
    let p = BinaryProtocol;
    let bytes = p.encode_request(7, &Request::Get { key: b"k".to_vec() });
    match p.decode_request(&bytes).unwrap() {
        DecodeOutcome::Complete { value, .. } => {
            assert_eq!(value, Request::Get { key: b"k".to_vec() });
        }
        DecodeOutcome::NeedMore => panic!(),
    }
}

#[test]
fn delete_roundtrip() {
    let p = BinaryProtocol;
    let bytes = p.encode_request(1, &Request::Delete { key: b"k".to_vec() });
    match p.decode_request(&bytes).unwrap() {
        DecodeOutcome::Complete { value, .. } => {
            assert_eq!(value, Request::Delete { key: b"k".to_vec() });
        }
        DecodeOutcome::NeedMore => panic!(),
    }
}

#[test]
fn empty_key_value() {
    let p = BinaryProtocol;
    let bytes = p.encode_request(0, &Request::Put {
        key: vec![],
        value: vec![],
    });
    match p.decode_request(&bytes).unwrap() {
        DecodeOutcome::Complete { value, .. } => {
            // ⭐ 空 payload decode 后 value = [TAG_RAW] (仅 1B tag)
            assert_eq!(value, Request::Put { key: vec![], value: vec![0x01] });
        }
        DecodeOutcome::NeedMore => panic!(),
    }
}

#[test]
fn incomplete_returns_need_more() {
    let p = BinaryProtocol;
    let full = p.encode_request(1, &Request::Put {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    });
    for cut in 0..full.len() {
        match p.decode_request(&full[..cut]).unwrap() {
            DecodeOutcome::NeedMore => {}
            DecodeOutcome::Complete { .. } => panic!("cut at {cut} should be need more"),
        }
    }
}

#[test]
fn invalid_opcode_errors() {
    let p = BinaryProtocol;
    let mut bytes = p.encode_request(1, &Request::Get { key: b"k".to_vec() });
    bytes[12] = 99; // corrupt op
    match p.decode_request(&bytes) {
        Err(ProtocolError::InvalidOpcode(99)) => {}
        other => panic!("expected InvalidOpcode(99), got {other:?}"),
    }
}

#[test]
fn frame_too_large_errors() {
    let p = BinaryProtocol;
    let mut bytes = p.encode_request(1, &Request::Get { key: b"k".to_vec() });
    let total_len = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    bytes[0..4].copy_from_slice(&(total_len + p.max_frame_size() as u32).to_be_bytes());
    match p.decode_request(&bytes) {
        Err(ProtocolError::FrameTooLarge { .. }) => {}
        other => panic!("expected FrameTooLarge, got {other:?}"),
    }
}

#[test]
fn response_put_ok_roundtrip() {
    let p = BinaryProtocol;
    let bytes = p.encode_response(99, &Response::PutOk);
    match p.decode_response(&bytes).unwrap() {
        DecodeOutcome::Complete { value, .. } => assert_eq!(value, Response::PutOk),
        DecodeOutcome::NeedMore => panic!(),
    }
}

#[test]
fn response_get_some_roundtrip() {
    let p = BinaryProtocol;
    let bytes = p.encode_response(99, &Response::Get(Some(b"data".to_vec())));
    match p.decode_response(&bytes).unwrap() {
        DecodeOutcome::Complete { value, .. } => {
            assert_eq!(value, Response::Get(Some(b"data".to_vec())));
        }
        DecodeOutcome::NeedMore => panic!(),
    }
}

#[test]
fn response_get_none_roundtrip() {
    let p = BinaryProtocol;
    let bytes = p.encode_response(99, &Response::Get(None));
    match p.decode_response(&bytes).unwrap() {
        DecodeOutcome::Complete { value, .. } => assert_eq!(value, Response::Get(None)),
        DecodeOutcome::NeedMore => panic!(),
    }
}

#[test]
fn response_error_roundtrip() {
    let p = BinaryProtocol;
    let bytes = p.encode_response(99, &Response::Error("boom".to_string()));
    match p.decode_response(&bytes).unwrap() {
        DecodeOutcome::Complete { value, .. } => {
            assert_eq!(value, Response::Error("boom".to_string()));
        }
        DecodeOutcome::NeedMore => panic!(),
    }
}
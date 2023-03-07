use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, RequestMode, Response};


const WASM_MEMORY_BUFFER_SIZE: usize = 2;
static mut WASM_MEMORY_BUFFER: [u8; WASM_MEMORY_BUFFER_SIZE] = [0; WASM_MEMORY_BUFFER_SIZE];

#[wasm_bindgen(js_name = addValue)]
pub fn add_value(a: i32, b: i32) -> i32 {
    return a + b;
}

#[wasm_bindgen(js_name = addValueToBuffer)]
pub fn add_value_to_buffer(a: u8){
    unsafe {
        WASM_MEMORY_BUFFER[0] = a;
    }
}

#[wasm_bindgen(js_name = getValueFromBuffer)]
pub fn get_value_from_buffer() -> u8{
    unsafe{
        return WASM_MEMORY_BUFFER[0];
    }
}

#[wasm_bindgen(js_name = getValueFromBufferIndex)]
//pub fn get_value_from_buffer_index(index: u32) -> u8{
pub fn get_value_from_buffer_index() -> u8{
    unsafe{
        return WASM_MEMORY_BUFFER[1];
    }
}

#[wasm_bindgen(js_name = getWasmMemoryBufferPointer)]
pub fn get_wasm_memory_buffer_pointer() -> *const u8 {
    let pointer: *const u8;
    unsafe {
        pointer = WASM_MEMORY_BUFFER.as_ptr();
    }

    return pointer;
}

#[wasm_bindgen(js_name = fetchTile)]
pub async fn fetch_tile(repo: String) -> Result<JsValue, JsValue>{
    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let url = format!("https://api.github.com/repos/{}/branches/master", repo);

    let request = Request::new_with_str_and_init(&url, &opts)?;

    request
        .headers()
        .set("Accept", "application/vnd.github.v3+json")?;

    let window = web_sys::window().unwrap();
    let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;

    // `resp_value` is a `Response` object.
    assert!(resp_value.is_instance_of::<Response>());
    let resp: Response = resp_value.dyn_into().unwrap();

    // Convert this other `Promise` into a rust `Future`.
    let json = JsFuture::from(resp.json()?).await?;

    // Send the JSON response back to JS.
    Ok(json)
}


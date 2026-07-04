pub fn calculate(from_x: i32, from_y: i32, to_x: i32, to_y: i32) -> i32 {
    match (to_x - from_x, to_y - from_y) {
        (1, 0) => 2,
        (-1, 0) => 6,
        (0, 1) => 4,
        (0, -1) => 0,
        (1, 1) => 3,
        (1, -1) => 1,
        (-1, 1) => 5,
        (-1, -1) => 7,
        _ => 0,
    }
}

use crate::virtuals::rooms::pathfinder::coord::Coord;

pub fn get_next_step(x: i32, y: i32, goal_x: i32, goal_y: i32) -> Coord {
    if x > goal_x && y > goal_y {
        Coord::new(x - 1, y - 1)
    } else if x < goal_x && y < goal_y {
        Coord::new(x + 1, y + 1)
    } else if x > goal_x && y < goal_y {
        Coord::new(x - 1, y + 1)
    } else if x < goal_x && y > goal_y {
        Coord::new(x + 1, y - 1)
    } else if x > goal_x {
        Coord::new(x - 1, y)
    } else if x < goal_x {
        Coord::new(x + 1, y)
    } else if y < goal_y {
        Coord::new(x, y + 1)
    } else if y > goal_y {
        Coord::new(x, y - 1)
    } else {
        Coord::new(-1, -1)
    }
}

#[cfg(test)]
mod tests {
    use super::get_next_step;
    use crate::virtuals::rooms::pathfinder::coord::Coord;

    #[test]
    fn next_step_matches_legacy_diagonal_preference() {
        assert_eq!(get_next_step(1, 1, 3, 3), Coord::new(2, 2));
        assert_eq!(get_next_step(3, 3, 1, 1), Coord::new(2, 2));
        assert_eq!(get_next_step(2, 2, 2, 2), Coord::new(-1, -1));
    }
}

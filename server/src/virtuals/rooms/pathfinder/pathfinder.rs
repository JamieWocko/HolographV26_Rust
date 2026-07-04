use crate::virtuals::rooms::virtual_room::SquareState;

#[derive(Debug, Clone)]
pub struct Pathfinder<'a> {
    state_map: &'a [Vec<SquareState>],
    height_map: &'a [Vec<u8>],
    unit_map: &'a [Vec<bool>],
    max_x: usize,
    max_y: usize,
}

#[derive(Debug, Clone)]
struct MapNode {
    x: i32,
    y: i32,
    cost: f64,
    parent: Option<usize>,
}

impl<'a> Pathfinder<'a> {
    pub fn new(
        state_map: &'a [Vec<SquareState>],
        height_map: &'a [Vec<u8>],
        unit_map: &'a [Vec<bool>],
    ) -> Self {
        let max_x = unit_map.len().saturating_sub(1);
        let max_y = unit_map
            .first()
            .map(|row| row.len().saturating_sub(1))
            .unwrap_or(0);

        Self {
            state_map,
            height_map,
            unit_map,
            max_x,
            max_y,
        }
    }

    pub fn get_next(&self, x: i32, y: i32, goal_x: i32, goal_y: i32) -> Option<(i32, i32)> {
        if x == goal_x && y == goal_y {
            return None;
        }

        let max_cycles = self.max_x.saturating_mul(self.max_y).max(1);
        let mut cycles = 0usize;
        let mut nodes = vec![MapNode {
            x,
            y,
            cost: 0.0,
            parent: None,
        }];
        let mut open = vec![0usize];
        let mut closed = Vec::<usize>::new();

        while !open.is_empty() {
            cycles += 1;
            if cycles >= max_cycles {
                return None;
            }

            let current_pos = self.lowest_total_cost_index(&nodes, &open, goal_x, goal_y)?;
            let current_index = open.remove(current_pos);
            let current = nodes[current_index].clone();

            if current.x == goal_x && current.y == goal_y {
                return self.first_step(&nodes, current_index);
            }

            for (next_x, next_y) in self.successors(current.x, current.y) {
                let next_cost = current.cost + 1.0;
                let existing_open = open
                    .iter()
                    .copied()
                    .find(|&index| nodes[index].x == next_x && nodes[index].y == next_y);
                if let Some(index) = existing_open
                    && next_cost >= nodes[index].cost
                {
                    continue;
                }

                let existing_closed = closed
                    .iter()
                    .copied()
                    .find(|&index| nodes[index].x == next_x && nodes[index].y == next_y);
                if let Some(index) = existing_closed
                    && next_cost >= nodes[index].cost
                {
                    continue;
                }

                if let Some(index) = existing_open {
                    open.retain(|entry| *entry != index);
                }
                if let Some(index) = existing_closed {
                    closed.retain(|entry| *entry != index);
                }

                nodes.push(MapNode {
                    x: next_x,
                    y: next_y,
                    cost: next_cost,
                    parent: Some(current_index),
                });
                open.push(nodes.len() - 1);
            }

            closed.push(current_index);
        }

        None
    }

    fn first_step(&self, nodes: &[MapNode], goal_index: usize) -> Option<(i32, i32)> {
        let mut path = Vec::new();
        let mut current = Some(goal_index);
        while let Some(index) = current {
            path.push((nodes[index].x, nodes[index].y));
            current = nodes[index].parent;
        }
        path.reverse();
        path.get(1).copied()
    }

    fn lowest_total_cost_index(
        &self,
        nodes: &[MapNode],
        open: &[usize],
        goal_x: i32,
        goal_y: i32,
    ) -> Option<usize> {
        open.iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| {
                self.total_cost(&nodes[**left], goal_x, goal_y)
                    .partial_cmp(&self.total_cost(&nodes[**right], goal_x, goal_y))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(position, _)| position)
    }

    fn total_cost(&self, node: &MapNode, goal_x: i32, goal_y: i32) -> f64 {
        let dx = (node.x - goal_x) as f64;
        let dy = (node.y - goal_y) as f64;
        node.cost + (dx * dx + dy * dy).sqrt()
    }

    fn successors(&self, x: i32, y: i32) -> Vec<(i32, i32)> {
        let mut successors = Vec::new();
        self.add_successor(&mut successors, x, y, x - 1, y);
        self.add_successor(&mut successors, x, y, x + 1, y);
        self.add_successor(&mut successors, x, y, x, y - 1);
        self.add_successor(&mut successors, x, y, x, y + 1);

        if self.spot_open(x, y - 1) {
            if self.spot_open(x - 1, y) {
                self.add_successor(&mut successors, x, y, x - 1, y - 1);
            }
            if self.spot_open(x + 1, y) {
                self.add_successor(&mut successors, x, y, x + 1, y - 1);
            }
        }

        if self.spot_open(x + 1, y) && self.spot_open(x, y + 1) {
            self.add_successor(&mut successors, x, y, x + 1, y + 1);
        }
        if self.spot_open(x - 1, y) && self.spot_open(x, y - 1) {
            self.add_successor(&mut successors, x, y, x - 1, y + 1);
        }

        successors
    }

    fn add_successor(
        &self,
        successors: &mut Vec<(i32, i32)>,
        from_x: i32,
        from_y: i32,
        to_x: i32,
        to_y: i32,
    ) {
        if !self.spot_open(to_x, to_y) || !self.spot_move_height_ok(from_x, from_y, to_x, to_y) {
            return;
        }

        successors.push((to_x, to_y));
    }

    fn spot_open(&self, x: i32, y: i32) -> bool {
        let (Ok(x), Ok(y)) = (usize::try_from(x), usize::try_from(y)) else {
            return false;
        };

        let Some(state_column) = self.state_map.get(x) else {
            return false;
        };
        let Some(unit_column) = self.unit_map.get(x) else {
            return false;
        };

        if *unit_column.get(y).unwrap_or(&true) {
            return false;
        }

        matches!(
            state_column.get(y).copied(),
            Some(SquareState::Open | SquareState::Rug)
        )
    }

    fn spot_move_height_ok(&self, x: i32, y: i32, goal_x: i32, goal_y: i32) -> bool {
        let (Ok(x), Ok(y), Ok(goal_x), Ok(goal_y)) = (
            usize::try_from(x),
            usize::try_from(y),
            usize::try_from(goal_x),
            usize::try_from(goal_y),
        ) else {
            return false;
        };

        let old_height = self
            .height_map
            .get(x)
            .and_then(|column| column.get(y))
            .copied();
        let new_height = self
            .height_map
            .get(goal_x)
            .and_then(|column| column.get(goal_y))
            .copied();

        match (old_height, new_height) {
            (Some(old_height), Some(new_height)) => {
                let old_height = i32::from(old_height);
                let new_height = i32::from(new_height);
                (new_height - old_height).abs() <= 1
            }
            _ => false,
        }
    }
}

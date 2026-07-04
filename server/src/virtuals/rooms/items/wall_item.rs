use crate::core::state::AppState;
use crate::managers::catalogue_manager;

#[derive(Debug, Clone)]
pub struct WallItem {
    pub id: i64,
    pub template_id: i64,
    pub wall_position: String,
    pub var: String,
}

impl WallItem {
    pub async fn new(
        state: &AppState,
        id: i64,
        template_id: i64,
        wall_position: String,
        var: String,
    ) -> Self {
        let resolved_var = if var.is_empty() {
            catalogue_manager::get_template(state, template_id)
                .await
                .colour
        } else {
            var
        };

        Self {
            id,
            template_id,
            wall_position,
            var: resolved_var,
        }
    }

    pub async fn sprite(&self, state: &AppState) -> String {
        catalogue_manager::get_template(state, self.template_id)
            .await
            .sprite
    }

    pub async fn to_legacy_string(&self, state: &AppState) -> String {
        format!(
            "{}\t{}\t \t{}\t{}",
            self.id,
            self.sprite(state).await,
            self.wall_position,
            self.var
        )
    }
}

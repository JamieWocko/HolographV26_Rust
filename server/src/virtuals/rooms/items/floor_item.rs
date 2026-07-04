use crate::core::state::AppState;
use crate::encoding::jeax_encoding::encode_vl64;
use crate::managers::catalogue_manager;

#[derive(Debug, Clone)]
pub struct FloorItem {
    pub id: i64,
    pub template_id: i64,
    pub x: i32,
    pub y: i32,
    pub z: u8,
    pub h: f64,
    pub var: String,
}

impl FloorItem {
    pub fn new(id: i64, template_id: i64, x: i32, y: i32, z: i32, h: f64, var: String) -> Self {
        Self {
            id,
            template_id,
            x,
            y,
            z: z as u8,
            h,
            var,
        }
    }

    pub async fn sprite(&self, state: &AppState) -> String {
        catalogue_manager::get_template(state, self.template_id)
            .await
            .sprite
    }

    pub async fn to_legacy_string(&self, state: &AppState) -> String {
        let template = catalogue_manager::get_template(state, self.template_id).await;
        let sep = '\u{2}';
        if template.sprite == "song_disk" {
            format!(
                "{}{sep}{}{sep}{}{}{}{}{}{}{sep}{}{sep}{sep}{}{sep}",
                self.id,
                template.sprite,
                encode_vl64(self.x),
                encode_vl64(self.y),
                encode_vl64(template.length as i32),
                encode_vl64(template.width as i32),
                encode_vl64(self.z as i32),
                format_height(self.h),
                template.colour,
                self.var
            )
        } else {
            format!(
                "{}{sep}{}{sep}{}{}{}{}{}{}{sep}{}{sep}{sep}H{}{sep}",
                self.id,
                template.sprite,
                encode_vl64(self.x),
                encode_vl64(self.y),
                encode_vl64(template.length as i32),
                encode_vl64(template.width as i32),
                encode_vl64(self.z as i32),
                format_height(self.h),
                template.colour,
                self.var
            )
        }
    }
}

fn format_height(value: f64) -> String {
    let mut out = value.to_string();
    if out.contains(',') {
        out = out.replace(',', ".");
    }
    out
}

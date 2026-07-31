use crate::api::{AppState, GitInfo};
use crate::domain::Repository;
use axum::Json;
use axum::extract::State;

pub async fn info<R: Repository + 'static>(State(app_state): State<AppState<R>>) -> Json<AppInfo> {
    Json(app_state.into())
}

#[derive(serde::Serialize)]
pub struct AppInfo {
    name: String,
    version: String,
    environment: String,
    git_info: GitInfoDto,
}

#[derive(serde::Serialize)]
pub struct GitInfoDto {
    branch: String,
    commit_id: String,
}

impl<R: Repository + 'static> From<AppState<R>> for AppInfo {
    fn from(val: AppState<R>) -> Self {
        Self {
            name: val.cfg.app_name().to_string(),
            version: val.cfg.app_version().to_string(),
            environment: val.cfg.environment().to_string(),
            git_info: val.git_info.into(),
        }
    }
}

impl From<GitInfo> for GitInfoDto {
    fn from(val: GitInfo) -> Self {
        Self {
            branch: val.branch,
            commit_id: val.commit_id,
        }
    }
}

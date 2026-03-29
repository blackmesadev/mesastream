use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Track {
    pub id: u64,
    pub title: String,
    pub duration: Option<u64>,
    pub media: Media,
    pub user: User,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Media {
    pub transcodings: Vec<Transcoding>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Transcoding {
    pub url: String,
    pub format: Format,
    pub quality: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Format {
    pub protocol: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct User {
    pub username: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AudioResponse {
    pub url: String, // url to audio to be downloaded
}

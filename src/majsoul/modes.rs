use anyhow::{Context, Result};

pub fn room_modes(room: &str, modes: &[String], players: u8) -> Result<Vec<i32>> {
    if !matches!(players, 3 | 4) {
        anyhow::bail!("players must be 3 or 4");
    }
    if modes.is_empty()
        || modes
            .iter()
            .any(|mode| !matches!(mode.as_str(), "east" | "south"))
    {
        anyhow::bail!("modes must contain east and/or south");
    }
    let rooms: Vec<&str> = match room {
        "all" => vec!["gold", "jade", "throne"],
        "gold" => vec!["gold"],
        "jade" => vec!["jade"],
        "throne" => vec!["throne"],
        _ => anyhow::bail!("unsupported room {room}"),
    };
    let mut result = Vec::new();
    for room in rooms {
        let (east, south) = match (players, room) {
            (4, "gold") => (8, 9),
            (4, "jade") => (11, 12),
            (4, "throne") => (15, 16),
            (3, "gold") => (21, 22),
            (3, "jade") => (23, 24),
            (3, "throne") => (25, 26),
            _ => unreachable!("validated room and player count"),
        };
        for mode in modes {
            result.push(if mode == "east" { east } else { south });
        }
    }
    result.sort_unstable();
    result.dedup();
    Ok(result)
}

pub fn mode_metadata(mode: i32) -> Result<(&'static str, &'static str, u8)> {
    match mode {
        8 => Ok(("gold", "east", 4)),
        9 => Ok(("gold", "south", 4)),
        11 => Ok(("jade", "east", 4)),
        12 => Ok(("jade", "south", 4)),
        15 => Ok(("throne", "east", 4)),
        16 => Ok(("throne", "south", 4)),
        21 => Ok(("gold", "east", 3)),
        22 => Ok(("gold", "south", 3)),
        23 => Ok(("jade", "east", 3)),
        24 => Ok(("jade", "south", 3)),
        25 => Ok(("throne", "east", 3)),
        26 => Ok(("throne", "south", 3)),
        _ => anyhow::bail!("unsupported ranked mode_id {mode}"),
    }
}

pub fn uuid_year(uuid: &str) -> Result<i32> {
    let date = uuid.get(0..6).context("full UUID has no YYMMDD prefix")?;
    let year = date
        .get(0..2)
        .context("full UUID has invalid date prefix")?
        .parse::<i32>()
        .with_context(|| format!("invalid year in full UUID {uuid}"))?;
    Ok(2000 + year)
}

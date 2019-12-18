package main

import "github.com/fatih/color"

func opFixMultipleCompletedScoresRX() {
	defer wg.Done()
	const initQuery = "SELECT id, userid, beatmap_md5, play_mode, score, pp FROM scores_relax WHERE completed = 3 ORDER BY id DESC"
	scores := []scoreRX{}
	rows, err := db.Query(initQuery)
	if err != nil {
		queryError(err, initQuery)
		return
	}
	for rows.Next() {
		currentScore := scoreRX{}
		rows.Scan(
			&currentScore.id,
			&currentScore.userid,
			&currentScore.beatmapMD5,
			&currentScore.score,
			&currentScore.pp,
			&currentScore.playMode,
		)
		scores = append(scores, currentScore)
	}
	verboseln("> FixMultipleCompletedScoresRX: Fetched, now finding bugged completed scores...")

	fixed := []int{}
	for i := 0; i < len(scores); i++ {
		if i%1000 == 0 {
			verboseln("> FixMultipleCompletedScoresRX:", i)
		}
		if contains(fixed, scores[i].id) {
			continue
		}
		for j := i + 1; j < len(scores); j++ {
			if contains(fixed, scores[j].id) {
				continue
			}
			if scores[j].id != scores[i].id && scores[j].beatmapMD5 == scores[i].beatmapMD5 && scores[j].userid == scores[i].userid && scores[j].playMode == scores[i].playMode {
				verbosef("> FixMultipleCompletedScoresRX: Found duplicated completed score (%d/%d)\n", scores[i].id, scores[j].id)
				if scores[j].pp > scores[i].pp {
					op("UPDATE scores_relax SET completed = 2 WHERE id = ?", scores[i].id)
				} else {
					op("UPDATE scores_relax SET completed = 2 WHERE id = ?", scores[j].id)
				}
				fixed = append(fixed, scores[i].id, scores[j].id)
			}
		}
	}

	color.Green("> FixMultipleCompletedScores [RELAX]: done!")
}

package main

import "github.com/fatih/color"

type score struct {
	id         int
	beatmapMD5 string
	userid     int
	score      int64
	maxCombo   int
	mods       int
	playMode   int
	accuracy   float64
	pp         float64
}

func (s score) sameAs(t score) bool {
	return s.beatmapMD5 == t.beatmapMD5 &&
		s.userid == t.userid &&
		s.score == t.score &&
		s.maxCombo == t.maxCombo &&
		s.mods == t.mods &&
		s.playMode == t.playMode &&
		s.accuracy == t.accuracy
		s.pp == t.pp
}

func opFixScoreDuplicatesRX() {
	defer wg.Done()
	const initQuery = "SELECT id, beatmap_md5, userid, score, max_combo, mods, play_mode, accuracy, pp FROM scores_relax WHERE completed = '3'"
	scores := []pp{}
	rows, err := db.Query(initQuery)
	if err != nil {
		queryError(err, initQuery)
		return
	}
	for rows.Next() {
		currentScore := pp{}
		rows.Scan(
			&currentScore.id,
			&currentScore.beatmapMD5,
			&currentScore.userid,
			&currentScore.score,
			&currentScore.maxCombo,
			&currentScore.mods,
			&currentScore.playMode,
			&currentScore.accuracy,
			&currentScore.pp,
		)
		scores = append(scores, currentScore)
	}

	verboseln("> FixScoreDuplicatesRX: Fetched, now finding duplicates")

	// duplicate removing
	remove := []int{}
	var ops int64
	for i := 0; i < len(scores); i++ {
		if contains(remove, scores[i].id) {
			continue
		}
		for j := i + 1; j < len(scores); j++ {
			if ops%5000000 == 0 {
				verboseln("> FixScoreDuplicatesRX:", ops)
			}
			if scores[i].sameAs(scores[j]) && !contains(remove, scores[j].id) {
				verboseln("> FixScoreDuplicatesRX: found one!")
				remove = append(remove, scores[j].id)
			}
			ops++
		}
	}

	for _, v := range remove {
		op("DELETE FROM scores WHERE id = ?", v)
	}
	color.Green("> FixScoreDuplicates [RELAX]: done!")
}

func containsRX(arr []int, i int) bool {
	for _, v := range arr {
		if v == i {
			return true
		}
	}
	return false
}

drop function if exists teams_oppositions;

create or replace function teams_oppositions(
    in team varchar(100)/*,
	in side ranking_type*/
)
returns table(
	"Team" varchar(100),
    "Opponent" varchar(100),
    "Matches" bigint,
    "Wins" bigint,
    "Draws" bigint,
    "Loses" bigint,
    "Goals For" bigint,
    "Goals Against" bigint,
    "Shots" bigint,
    "Shots on Target" bigint,
    "Yellow Cards" bigint,
    "Incl. 2 Yellow Cards" bigint,
    "Red Cards" bigint
)
as $$
begin
    return query

    with oppositions as (
        select
            case 
                when played_home then m.home_team
                else m.away_team
            end as team,
            case 
                when played_home then m.away_team
                else m.home_team
            end as opponent,

            case
                when played_home then home_score
                else away_score
            end as goals_for,
            case 
                when not played_home then home_score
                else away_score
            end as goals_against,

            case
                when played_home and home_score > away_score then 1
                when not played_home and away_score > home_score then 1
                else 0
            end as win,
            case
                when home_score = away_score then 1
                else 0
            end as draw,
            case
                when played_home and home_score < away_score then 1
                when not played_home and away_score < home_score then 1
                else 0
            end as lose,

            shots,
            on_target,
            y_cards,
            yr_cards,
            r_cards

        from match m
        join team_stats ts
        on m.id = ts.id_match
    )

    select
        t.complete_name as Team,
        o.complete_name as Opponent,

        sum(win + draw + lose) as Matches,
        sum(win) as Wins,
        sum(draw) as Draws,
        sum(lose) as Loses,

        sum(goals_for) as "Goals For",
        sum(goals_against) as "Goals Against",
        sum(shots) as Shots,

        sum(on_target) as "Shots on Target",
        sum(y_cards) as "Yellow Cards",
        sum(yr_cards) as "Incl. 2 Yellow Cards",
        sum(r_cards) as "Red Cards"

    from oppositions os
    join (select id, complete_name from club where complete_name = team) t
    on os.team = t.id
    join club o
    on os.opponent = o.id
    group by t.complete_name, o.complete_name;
end;
$$ language plpgsql;
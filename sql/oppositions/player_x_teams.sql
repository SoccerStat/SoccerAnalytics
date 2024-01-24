drop function if exists players_oppositions;

create or replace function players_oppositions(
    in player varchar(100),
    in id_chp varchar(20) default 'all',
    in id_season varchar(20) default 'all'/*,
	in side ranking_type*/
)
returns table(
	"Player" varchar(100),
    "Opponent" varchar(100),
    "Matches" bigint,
    "Wins" bigint,
    "Draws" bigint,
    "Loses" bigint,
    "Goals" bigint,
    "Assists" bigint,
    "Minutes" bigint,
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
            id_player,
            p.name as player,
            case 
                when played_home then m.home_team
                else m.away_team
            end as team,
            case 
                when played_home then m.away_team
                else m.home_team
            end as opponent,

            case
                when played_home and home_score > away_score then 1
                when not played_home and away_score > home_score then 1
                else 0
            end as "win",
            case
                when home_score = away_score then 1
                else 0
            end as "draw",
            case
                when played_home and home_score < away_score then 1
                when not played_home and away_score < home_score then 1
                else 0
            end as "lose",

            goals,
            assists,
            minutes,
            position,
            played_home,
            shots,
            shots_on_target,
            cards_yellow,
            cards_yellow_red,
            cards_red
        
        from player_stats ps
        join match m
        on ps.id_match = m.id
        join player p
        on ps.id_player = p.id
        where p.name = player
        and case when id_chp = 'all' then true else m.id_championship = id_chp end
        and case when id_season = 'all' then true else m.season = id_season end
    )
    
    select
        o.player,
        c.complete_name as "Opponent",
        sum(win + draw + lose) as "Matches",
        sum(win) as "Wins",
        sum(draw) as "Draws",
        sum(lose) as "Loses",
        sum(goals) as "Goals",
        sum(assists) as "Assists",
        sum(minutes) as "Minutes",
        sum(shots) as "Shots",
        sum(shots_on_target) as "Shots on Target",
        sum(cards_yellow) as "Yellow Cards",
        sum(cards_yellow_red) as "Incl. 2 Yellow Cards",
        sum(cards_red) as "Red Cards"
    
    from oppositions o
    join club c
    on o.opponent = c.id
    group by c.complete_name, o.player;
end;
$$ language plpgsql;
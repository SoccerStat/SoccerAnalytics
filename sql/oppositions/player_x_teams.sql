drop function if exists public.players_oppositions;

create or replace function public.players_oppositions(
    in player varchar(100),
    in id_comp varchar(20),
    in id_season varchar(20)/*,
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
    "Minutes/Match" numeric,
    "Shots" bigint,
    "Shots on Target" bigint,
    "Yellow Cards" bigint,
    "Incl. 2 Yellow Cards" bigint,
    "Red Cards" bigint
)
as $$
DECLARE
    season_schema text;
	query text;
begin
    season_schema = 'dwh_' || id_season;

    query := format(
        'with oppositions as (
            select
                p.id,
                p.name as player,
                m.competition,
                case 
                    when ps.played_home then m.home_team
                    else m.away_team
                end as team,
                case 
                    when ps.played_home then m.away_team
                    else m.home_team
                end as opponent,

                case
                    when ps.played_home and tsh.score > tsa.score then 1
                    when not ps.played_home and tsa.score > tsh.score then 1
                    else 0
                end as "win",
                case
                    when tsh.score = tsa.score then 1
                    else 0
                end as "draw",
                case
                    when ps.played_home and tsh.score < tsa.score then 1
                    when not ps.played_home and tsa.score < tsh.score then 1
                    else 0
                end as "lose",

                ps.nb_goals,
                ps.nb_assists,
                ps.nb_minutes,
                ps.position,
                ps.played_home,
                ps.nb_shots,
                ps.nb_shots_on_target,
                ps.nb_cards_yellow,
                ps.nb_cards_second_yellow,
                ps.nb_cards_red
            
            from %I.player_main_stats ps
            join %I.match m
            on ps.match = m.id
            join (select * from %I.team_stats where played_home) as tsh
            on tsh.match = m.id
            join (select * from %I.team_stats where not played_home) as tsa
            on tsa.match = m.id
            join dwh_upper.player p
            on ps.player = p.id
            where p.name = ''' || player || '''
            and case when ''' || id_comp || ''' = ''all'' then true else m.competition = ''' || id_comp || ''' end
        )
        select
            o.player,
            c.name as "Opponent",
            sum(win + draw + lose) as "Matches",
            sum(win) as "Wins",
            sum(draw) as "Draws",
            sum(lose) as "Loses",
            sum(nb_goals) as "Goals",
            sum(nb_assists) as "Assists",
            sum(nb_minutes) as "Minutes",
            round(sum(nb_minutes)/sum(win + draw + lose), 2) as "Minutes/Match",
            sum(nb_shots) as "Shots",
            sum(nb_shots_on_target) as "Shots on Target",
            sum(nb_cards_yellow) as "Yellow Cards",
            sum(nb_cards_second_yellow) as "Incl. 2 Yellow Cards",
            sum(nb_cards_red) as "Red Cards"
        
        from oppositions o
        join dwh_upper.club c
        on o.opponent = o.competition || ''_'' || c.id
        group by c.name, o.player;
        ',
        season_schema, season_schema,
        season_schema, season_schema
    );

    RETURN QUERY EXECUTE query USING id_comp, id_season;
end;
$$ language plpgsql;
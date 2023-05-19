select * from cf_apports();
drop function cf_apports(int, int, character varying);
create or replace function cf_apports(_annee int, _semaine int, _predict varchar(10))
returns table(
week int,
season int,
nombre_apports int,
nombre_jours_feries int,
t_min int,
t_max int,
pluie float
)
as $func$
begin

	drop table if exists trc;

	create temp table trc as(
		with agg1 as( -- compter le nombre d'apports par an et par semaine
		select 
		extract(week from date_recolte) as semaine,
		extract(year from date_recolte) as annee,
		count(code_adherent) as nbr_apports
		from cf_apports
		group by 1, 2
		), 
		agg2 as(    -- cumuls météo par an et par semaine
		select 
		extract(week from date_reelle) as semaine,
		extract(year from date_reelle) as annee,
		sum(tmax) as tmax, 
		sum(tmin) as tmin,
		sum(rr10) as rr10
		from meteo where code_insee=29030
		group by 1, 2
		), 
		agg3 as(  -- trouver les jours fériés travaillés ou non
			with sup_agg as(
				with inf_agg as(
				select jf.jour, 
				sum(cfa.quantite_recolte) as test_somme 
				from jours_feries jf 
				left join cf_apports cfa 
				on jf.jour=cfa.date_recolte 
				where jf.jour<=(select current_date)
				group by 1 
				order by 1)
			select 
			jour,
			extract(week from jour) as semaine,
			extract(year from jour) as annee,
			case when test_somme is not null then 1 end as travaille
			from inf_agg)	
		select 
		semaine, 
		annee, 
		sum(travaille) as nb_jr_ferie
		from sup_agg
		group by 1, 2 order by 1, 2)
	select agg2.semaine::int,
	agg2.annee::int,
	agg1.nbr_apports::int,
	agg3.nb_jr_ferie::int,
	agg2.tmin::int, 
	agg2.tmax::int,
	agg2.rr10::float
	from agg2 
	left join agg1 on agg1.semaine=agg2.semaine and agg1.annee=agg2.annee
	left join agg3 on agg3.semaine=agg2.semaine and agg3.annee=agg2.annee
	order by agg1.annee, agg2.semaine);

	update trc set nb_jr_ferie = 0 where nb_jr_ferie is null;

	if $3 = 'predict' then
		return query select * from trc where annee=$1 and semaine=$2;
	else
		return query select * from trc where annee<=$1 and semaine<=$2 order by annee, semaine;
	end if;

end;
$func$ language plpgsql;


-- --------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------
drop function if exists cf_pred_finale();
DROP FUNCTION cf_pred_finale(integer,integer,character varying);
create or replace function cf_pred_finale(_annee int, _semaine int, _predict varchar(10))
returns table(
saison int,
sem int,
pred int, 
qte_recolte int,
er_pct float,
dif_apports int,
quotient_apports float,
app_predits int,
app_reportes int,
dif_tmin int,
dif_tmax int, 
t_min int, 
t_max int
)
as $$
begin
	
	drop table if exists prediction_finale_chou_fleur;

	create temp table prediction_finale_chou_fleur as
	with tab as(
			with predictions as(
			select extract(week from jour) as semaine,
			extract(year from jour) as annee,
			sum(dt) as rf
			from cf_predictions 
			where jour>='2019-09-09'
			group by 1, 2
			), 
			reel as(
			select
			extract(week from date_recolte) as semaine,
			extract(year from date_recolte) as annee,
			sum(quantite_recolte) as reel
			from cf_apports
			where date_recolte>='2019-09-09'
			group by 1, 2
			), 
			nb_apports as(
			select
			cnb.semaine, 
			cnb.annee,
			cnb.apports_reportes,
			cnb.apports_predits,
			abs(cnb.apports_predits-cnb.apports_reportes) as diff_apports
			from cf_nb_apports cnb
			where annee>=2019
			), 
			mete as(
			with agg4 as( 
			select 
			extract(week from date_reelle) as semaine,
			extract(year from date_reelle) as annee,
			sum(tmax) as tmax, 
			sum(tmin) as tmin,
			sum(rr10) as rr10
			from meteo 
			where code_insee=29030
			group by 1, 2
			)
		select
		agg4.semaine, 
		agg4.annee,
		agg4.tmin, 
		lag(agg4.tmin, 1) over(order by agg4.semaine, agg4.annee) as diff_tmin,
		agg4.tmax,
		lag(agg4.tmax, 1) over(order by agg4.semaine, agg4.annee) as diff_tmax
		from agg4
		)
	select 
	m.annee::int,
	m.semaine::int,
	pr.rf::int as prediction, 
	r.reel::int as quantite_recolte,
	round(((abs(pr.rf-r.reel)/r.reel::float)*100)::numeric,2)::float as erreur_pct,
	abs(n.apports_predits-n.apports_reportes)::int as diff_apports,
	abs((n.apports_reportes - n.apports_predits)/n.apports_predits::float)::float as quotient,
	n.apports_predits::int,
	n.apports_reportes::int,
	m.diff_tmin::int as diff_tmin,
	m.diff_tmax::int as diff_tmax,
	m.tmin::int,
	m.tmax::int
	from predictions pr
	left join reel r on r.semaine=pr.semaine and r.annee=pr.annee 
	left join nb_apports n on n.semaine=pr.semaine and n.annee=pr.annee
	left join mete m on m.semaine=pr.semaine and m.annee=pr.annee
	where n.apports_predits is not null
	order by pr.annee, pr.semaine)
select * from tab;
	
	if $3='predict' then 
		return query select * from prediction_finale_chou_fleur where annee=$1 and semaine=$2;
	else
		return query select * from prediction_finale_chou_fleur;
	end if;
	
	drop table if exists prediction_finale_chou_fleur;
	
	
end;
$$ language plpgsql



-- --------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------

create view cf_stat as
with sup_agg as(
		with agg as(
		select extract(week from jour) as semaine,
		extract(year from jour) as annee,
		sum(dt) as rf
		from cf_predictions 
		--where jour>='2019-09-09'
		group by 1, 2
		), 
		agg2 as(
		select
		extract(week from date_recolte) as semaine,
		extract(year from date_recolte) as annee,
		sum(quantite_recolte) as reel
		from cf_apports
		--where date_recolte>='2019-09-09'
		group by 1, 2
		), 
		agg3 as( 
		select 
		extract(week from date_recolte) as semaine,
		extract(year from date_recolte) as annee,
		count(code_adherent) as nbr_apports
		from cf_apports
		--where date_recolte>='2019-09-09'
		group by 1, 2
		), 
		agg4 as( 
		select 
		extract(week from date_reelle) as semaine,
		extract(year from date_reelle) as annee,
		sum(tmax) as tmax, 
		sum(tmin) as tmin,
		sum(rr10) as rr10
		from meteo 
		where code_insee=29030
		group by 1, 2
		), 
		agg5 as(
		select 
		annee, 
		semaine,
		pred as pred_finale
		from cf_pred_finale
		)
	select agg4.semaine,
	agg4.annee,
	agg.rf,
	agg5.pred_finale,
	agg2.reel,
	case when tmin<10 and (lag(tmin) over(order by agg4.semaine, agg4.annee)<30) then (agg.rf/2)::int
	when tmin<10 and (lag(tmin) over(order by agg4.semaine, agg4.annee)<50) then (agg.rf/1.5)::int
	when tmin <25 then (rf/1.4)::int
	else (rf)::int end as pond,
	--(abs(agg.rf-agg2.reel)/agg2.reel::float)*100 as erreur_pct,
	agg3.nbr_apports,
	agg3.nbr_apports - (lag(nbr_apports, 1) over(order by agg3.annee, agg3.semaine)) as diff_apports,
	lag(nbr_apports, 1) over(order by agg3.annee, agg3.semaine) as apports_semaine_derniere,
	agg4.tmin, 
	agg4.tmin - lag(agg4.tmin, 1) over(order by agg4.annee, agg4.annee) as diff_tmin,
	agg4.tmax,
	agg4.tmax - lag(agg4.tmax, 1) over(order by agg4.annee, agg4.semaine) as diff_tmax,
	agg4.rr10
	from agg4 
	left join agg2 on agg4.semaine=agg2.semaine and agg4.annee=agg2.annee
	left join agg3 on agg4.semaine=agg3.semaine and agg4.annee=agg3.annee 
	left join agg on agg.semaine=agg4.semaine and agg.annee=agg4.annee
	left join agg5 on agg5.semaine=agg4.semaine and agg5.annee=agg4.annee
	order by agg.semaine
	)
select 
sa.annee,
sa.semaine,
sa.rf as pred_iter1, 
sa.pred_finale as pred_iter2,
sa.reel as quantite_recolte,
round(((abs(sa.rf-sa.reel)/sa.reel::float)*100)::numeric,2) as erreur_pct_iter1,
round(((abs(sa.pred_finale-sa.reel)/sa.reel::float)*100)::numeric,2) as erreur_pct_iter2,
sa.tmin,
sa.diff_tmin,
sa.tmax,
sa.diff_tmax,
sa.rr10,
cnb.apports_reportes,
cnb.apports_predits,
cnb.apports_predits-cnb.apports_reportes as diff_apports
from sup_agg sa
left join cf_nb_apports cnb on cnb.semaine=sa.semaine and cnb.annee=sa.annee 
where sa.pred_finale is not null
order by annee, semaine;

-- --------------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------------
-- 30/12/2022

CREATE OR REPLACE FUNCTION public.cf3(_flux character varying, _jour date)
 RETURNS TABLE(liv_date date, jr_saison integer, qte_recolte integer, quantite_initiale integer, t_min double precision, t_max double precision, pluie double precision, cumul_tmax7 double precision, cumul_tmax38 double precision, cumul_tmax60 double precision, cumul_tmin8 double precision, cumul_tmin35 double precision, cumul_tmin60 double precision, cumul_jourfroid7 double precision, cumul_jourfroid35 double precision, cumul_jourfroid60 double precision, cumul_jourchaud8 double precision, cumul_jourchaud38 double precision, cumul_jourchaud60 double precision, cumul_pluie7 double precision, cumul_pluie37 double precision, cumul_pluie60 double precision, cumul_jourdoux9 double precision, cumul_jourdoux38 double precision, cumul_jourdoux60 double precision, cumul_jour_de_pluie7 double precision, cumul_jour_de_pluie37 double precision, cumul_jour_de_pluie60 double precision, gel7 integer, gel38 integer, gel60 integer, jtf7 integer, jtf38 integer, jtf60 integer, pm7 integer, pm38 integer, pm60 integer, pf7 integer, pf38 integer, pf60 integer, fr7 integer, fr38 integer, df60 integer)
 LANGUAGE plpgsql
AS $function$
	begin 
	
			drop table if exists cf_apports_semis;
			drop table if exists meteo_cf2_corr;
			drop table if exists truc23;
			drop table if exists s1;
			drop table if exists cf_fe_apports_semis;
			drop table if exists apports_semis_agg;
			drop table if exists cf_semis_saison;	
			drop table if exists cf_apports_saison;
		
		create table meteo_cf2_corr as
		with agg as(
			select
			meteo.*,
			CASE WHEN tmin<0 THEN 1 ELSE 0 END AS gel,
			CASE WHEN tmin<5 THEN 1 ELSE 0 END AS froid,
			case when tmax<10 then 1 else 0 end as jourfroid,
			CASE WHEN tmax<5 THEN 1 ELSE 0 END AS jourtresfroid,
			CASE WHEN (tmax>=15 AND tmax<=25) THEN 1 ELSE 0 END AS jourchaud,
			CASE WHEN (tmax>=15 AND tmax<=20) THEN 1 ELSE 0 END AS jourdoux,
			CASE WHEN (rr10>0) THEN 1 ELSE 0 END AS arrosage,
			CASE WHEN (rr10>8) THEN 1 ELSE 0 END AS pluiemoderee,
			CASE WHEN (rr10>15) THEN 1 ELSE 0 END AS pluieforte
			from meteo
			where code_insee=29030
		)
		select
		date_reelle,
		tmax,
		tmin,
		rr10,
		jourchaud,
		jourdoux,
		arrosage,
		lag(tmax, 1) over (order by code_insee, date_reelle) as t1,
		lag(tmax, 2) over (order by code_insee, date_reelle) as t2,
		lag(tmax, 4) over (order by code_insee, date_reelle) as t4,
		lag(rr10, 1) over (order by code_insee, date_reelle) as p1,
		lag(rr10, 2) over (order by code_insee, date_reelle) as p2,
		lag(rr10, 4) over (order by code_insee, date_reelle) as p4,
		lag(rr10, 7) over (order by code_insee, date_reelle) as p7,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumultmax2,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumultmax3,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumultmax4,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumultmax5,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumultmax7,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 10 preceding and 1 preceding) as cumultmax9,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 14 preceding and 1 preceding) as cumultmax13,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 19 preceding and 1 preceding) as cumultmax18,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 23 preceding and 1 preceding) as cumultmax22,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 30 preceding and 1 preceding) as cumultmax29,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumultmax38,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumultmax60,
--		sum(tmax) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumultmax90,
--		sum(tmax) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumultmax120,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumultmin2,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumultmin3,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumultmin4,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumultmin5,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 9 preceding and 1 preceding) as cumultmin8,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 13 preceding and 1 preceding) as cumultmin12,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 20 preceding and 1 preceding) as cumultmin19,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 26 preceding and 1 preceding) as cumultmin25,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 30 preceding and 1 preceding) as cumultmin29,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 36 preceding and 1 preceding) as cumultmin35,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumultmin60,
--		sum(tmin) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumultmin90,
--		sum(tmin) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumultmin120,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumulrr102,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulrr103,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulrr104,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulrr105,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulrr107,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 11 preceding and 1 preceding) as cumulrr1010,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 18 preceding and 1 preceding) as cumulrr1017,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 28 preceding and 1 preceding) as cumulrr1027,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 38 preceding and 1 preceding) as cumulrr1037,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulrr1060,
--		sum(rr10) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumulrr1090,
--		sum(rr10) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumulrr10120,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljourfroid4,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumuljourfroid7,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 14 preceding and 1 preceding) as cumuljourfroid13,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 19 preceding and 1 preceding) as cumuljourfroid18,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 29 preceding and 1 preceding) as cumuljourfroid28,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 36 preceding and 1 preceding) as cumuljourfroid35,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljourfroid60,
--		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumuljourfroid90,
--		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumuljourfroid120,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumuljourchaud2,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljourchaud4,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumuljourchaud6,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 9 preceding and 1 preceding) as cumuljourchaud8,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 12 preceding and 1 preceding) as cumuljourchaud11,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 18 preceding and 1 preceding) as cumuljourchaud17,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 24 preceding and 1 preceding) as cumuljourchaud23,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 34 preceding and 1 preceding) as cumuljourchaud33,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumuljourchaud38,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljourchaud60,
--		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumuljourchaud90,
--		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumuljourchaud120,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljourdoux4,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 10 preceding and 1 preceding) as cumuljourdoux9,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 14 preceding and 1 preceding) as cumuljourdoux13,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 16 preceding and 1 preceding) as cumuljourdoux15,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 27 preceding and 1 preceding) as cumuljourdoux26,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumuljourdoux38,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljourdoux60,
--		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumuljourdoux90,
--		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumuljourdoux120,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumularrosage7,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 18 preceding and 1 preceding) as cumularrosage17,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 28 preceding and 1 preceding) as cumularrosage27,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 38 preceding and 1 preceding) as cumularrosage37,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumularrosage60,
--		sum(arrosage) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumularrosage90,
--		sum(arrosage) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumularrosage120
		sum(gel) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulgel3,
		sum(gel) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulgel4,
		sum(gel) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulgel5,
		sum(gel) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulgel6,
		sum(gel) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulgel7,
		sum(gel) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulgel38,
		sum(gel) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulgel60,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumuljtf3,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljtf4,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumuljtf5,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumuljtf6,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumuljtf7,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumuljtf38,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljtf60,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulpm3,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulpm4,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulpm5,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulpm6,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulpm7,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulpm38,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulpm60,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulpf3,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulpf4,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulpf5,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulpf6,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulpf7,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulpf38,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulpf60,
		sum(froid) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulfroid3,
		sum(froid) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulfroid4,
		sum(froid) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulfroid5,
		sum(froid) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulfroid6,
		sum(froid) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulfroid7,
		sum(froid) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulfroid38,
		sum(froid) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulfroid60
		from agg;
	
	
		create table s1 as
		with agg as(
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2016 as qte_semis, 2016 as annee from cf_ds_semis cds where extract(year from date_plantation)=2016 and surface_2016 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2017 as qte_semis, 2017 as annee from cf_ds_semis cds where extract(year from date_plantation)=2017 and surface_2017 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2018 as qte_semis, 2018 as annee from cf_ds_semis cds where extract(year from date_plantation)=2018 and surface_2018 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2019 as qte_semis, 2019 as annee from cf_ds_semis cds where extract(year from date_plantation)=2019 and surface_2019 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2020 as qte_semis, 2020 as annee from cf_ds_semis cds where extract(year from date_plantation)=2020 and surface_2020 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2021 as qte_semis, 2021 as annee from cf_ds_semis cds where extract(year from date_plantation)=2021 and surface_2021 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2022 as qte_semis, 2022 as annee from cf_ds_semis cds where extract(year from date_plantation)=2022 and surface_2022 is not null
		)
		select * from agg;

		create table cf_semis_saison as
		select s1.*,
		case
		when date_plantation<'2016-09-01' then 2015
		when date_plantation between '2015-05-15' and '2016-05-14' then 2016
		when date_plantation between '2016-05-15' and '2017-05-14' then 2017
		when date_plantation between '2017-05-15' and '2018-05-14' then 2018
		when date_plantation between '2018-05-15' and '2019-05-14' then 2019
		when date_plantation between '2019-05-15' and '2020-05-14' then 2020
		when date_plantation between '2020-05-15' and '2021-05-14' then 2021
		when date_plantation between '2021-05-15' and '2022-05-14' then 2022
		when date_plantation between '2023-05-15' and '2023-05-14' then 2023
		end as saison
		from s1;
	
		if $1='predict' then
			create table cf_apports_saison as 
			with agg as(
			select cf_apports_pred.*,
			case 
			when date_recolte<'2016-09-01' then 2015
			when date_recolte between '2016-09-01' and '2017-08-31' then 2016
			when date_recolte between '2017-09-01' and '2018-08-31' then 2017
			when date_recolte between '2018-09-01' and '2019-08-31' then 2018
			when date_recolte between '2019-09-01' and '2020-08-31' then 2019
			when date_recolte between '2020-09-01' and '2021-08-31' then 2020
			when date_recolte between '2021-09-01' and '2022-08-31' then 2021
			when date_recolte between '2022-09-01' and '2023-08-31' then 2022
			when date_recolte between '2023-09-01' and '2024-08-31' then 2023
			end as saison
			from cf_apports_pred)
			select 
			agg.date_recolte,
			agg.saison,
			agg.code_adherent, 
			sum(agg.quantite_recolte) as quantite_recolte
			from agg
			group by 1, 2, 3
			order by 1;
		else
			create table cf_apports_saison as 
			with agg as(
			select cf_apports.*,
			case 
			when date_recolte<'2016-09-01' then 2015
			when date_recolte between '2016-09-01' and '2017-08-31' then 2016
			when date_recolte between '2017-09-01' and '2018-08-31' then 2017
			when date_recolte between '2018-09-01' and '2019-08-31' then 2018
			when date_recolte between '2019-09-01' and '2020-08-31' then 2019
			when date_recolte between '2020-09-01' and '2021-08-31' then 2020
			when date_recolte between '2021-09-01' and '2022-08-31' then 2021
			when date_recolte between '2022-09-01' and '2023-08-31' then 2022
			when date_recolte between '2023-09-01' and '2024-08-31' then 2023
			end as saison
			from cf_apports)
			select 
			agg.date_recolte,
			agg.saison,
			agg.code_adherent,
			sum(agg.quantite_recolte) as quantite_recolte
			from agg
			group by 1, 2, 3
			order by 1;
		end if;	
		
		create table cf_apports_semis as
			with sup_agg as(
				with agg as( 
					select 
					code_adherent, 
					saison, 
					sum(qte_semis) as qte_semis
					from cf_semis_saison
					group by 1, 2
					order by 2)
				select 
				cfa.code_adherent,
				cfa.date_recolte,
				cfa.quantite_recolte,
				agg.qte_semis
				from cf_apports_saison cfa 
				left join agg
				on agg.saison=cfa.saison
				and cfa.code_adherent=agg.code_adherent
				order by 2)
			select 
			date_recolte, 
			sum(qte_semis) as qte_semis,
			sum(quantite_recolte) as quantite_recolte
			from sup_agg
			group by 1;
		
	
		
		
		
	if $1='predict' then
		create table apports_semis_agg as
		with b1 as(
			with sup_agg as(   -- lags de mi_recolte par semaine, cf, permettant des données pour les prédictions du jeudi
				with agg as( -- agg s1 à la semaine
					with s1 as( -- ajout saison apports, agg au jour, qté récolte mi_semaine
						select
						case 
						when date_recolte<'2016-09-01' then 2015
						when date_recolte between '2016-09-01' and '2017-08-31' then 2016
						when date_recolte between '2017-09-01' and '2018-08-31' then 2017
						when date_recolte between '2018-09-01' and '2019-08-31' then 2018
						when date_recolte between '2019-09-01' and '2020-08-31' then 2019
						when date_recolte between '2020-09-01' and '2021-08-31' then 2020
						when date_recolte between '2021-09-01' and '2022-08-31' then 2021
						when date_recolte >='2022-09-01' then 2022 end as saison,
						date_recolte,
						quantite_recolte 
						from cf_apports_pred)
					select 
					sum(quantite_recolte) as quantite_recolte,
					date_recolte, 
					saison
					from s1
					where extract(dow from date_recolte)<=4
					group by 2, 3)
				select 
				extract(week from date_recolte) as semaine,
				extract(year from date_recolte) as annee,
				saison,
				sum(quantite_recolte) as mi_week_quantite_recolte
				from agg 
				group by 1, 2, 3),
			sup_agg2 as(  -- agg apports à la semaine
				select 
				sum(quantite_recolte) as quantite_recolte,
				extract(week from date_recolte) as semaine,
				extract(year from date_recolte) as annee
				from cf_apports_pred
				group by 2, 3)
			select sup_agg.*,  -- feature engineering avec mi_recolte/semaine selon la saison
			sup_agg2.quantite_recolte,
			sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float as ratio,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 1)over(order by sup_agg.annee, sup_agg.semaine) as ratio_1,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 2)over(order by sup_agg.annee, sup_agg.semaine) as ratio_2,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 3)over(order by sup_agg.annee, sup_agg.semaine) as ratio_3,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 4)over(order by sup_agg.annee, sup_agg.semaine) as ratio_4,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 5)over(order by sup_agg.annee, sup_agg.semaine) as ratio_5,
			lag(sup_agg.mi_week_quantite_recolte, 1)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_1,
			lag(sup_agg.mi_week_quantite_recolte, 2)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_2,
			lag(sup_agg.mi_week_quantite_recolte, 3)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_3,
			lag(sup_agg.mi_week_quantite_recolte, 4)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_4,
			lag(sup_agg.mi_week_quantite_recolte, 5)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_5
			from sup_agg join sup_agg2
			on sup_agg.annee=sup_agg2.annee
			and sup_agg.semaine=sup_agg2.semaine 
			order by sup_agg.annee, sup_agg.semaine), 
		b2 as(
				with s2 as(   -- ajout saison aux apports
					select
					case 
					when date_recolte<'2016-09-01' then 2015
					when date_recolte between '2016-09-01' and '2017-08-31' then 2016
					when date_recolte between '2017-09-01' and '2018-08-31' then 2017
					when date_recolte between '2018-09-01' and '2019-08-31' then 2018
					when date_recolte between '2019-09-01' and '2020-08-31' then 2019
					when date_recolte between '2020-09-01' and '2021-08-31' then 2020
					when date_recolte between '2021-09-01' and '2022-08-31' then 2021
					when date_recolte >='2022-09-01' then 2022 end as saison,
					extract(week from date_recolte) as semaine,
					extract(year from date_recolte) as annee,
					sum(quantite_recolte) as quantite_recolte
					from cf_apports_pred
					group by 1, 2, 3 order by 1, 3, 2), 
				s3 as(  -- calcul semis puis ajout saison
					with aggs as(  -- calcul semis par producteur par an TODO --> saison aggs !!
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2016 as qte_semis, 2016 as annee from cf_ds_semis cds where extract(year from date_plantation)=2016 and surface_2016 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2017 as qte_semis, 2017 as annee from cf_ds_semis cds where extract(year from date_plantation)=2017 and surface_2017 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2018 as qte_semis, 2018 as annee from cf_ds_semis cds where extract(year from date_plantation)=2018 and surface_2018 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2019 as qte_semis, 2019 as annee from cf_ds_semis cds where extract(year from date_plantation)=2019 and surface_2019 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2020 as qte_semis, 2020 as annee from cf_ds_semis cds where extract(year from date_plantation)=2020 and surface_2020 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2021 as qte_semis, 2021 as annee from cf_ds_semis cds where extract(year from date_plantation)=2021 and surface_2021 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2022 as qte_semis, 2022 as annee from cf_ds_semis cds where extract(year from date_plantation)=2022 and surface_2022 is not null
					)
				select
				case 
				when date_plantation<'2016-09-01' then 2015
				when date_plantation between '2015-05-15' and '2016-05-14' then 2016
				when date_plantation between '2016-05-15' and '2017-05-14' then 2017
				when date_plantation between '2017-05-15' and '2018-05-14' then 2018
				when date_plantation between '2018-05-15' and '2019-05-14' then 2019
				when date_plantation between '2019-05-15' and '2020-05-14' then 2020
				when date_plantation between '2020-05-15' and '2021-05-14' then 2021
				when date_plantation between '2021-05-15' and '2022-05-14' then 2022
				when date_plantation between '2023-05-15' and '2023-05-14' then 2023
				end as saison,
				sum((qte_semis)*3)::int as qte_semis
				from aggs
				group by 1
				)
			select -- calcul semis restant par rapport aux apports
			s2.saison,
			s2.semaine,
			s2.annee,
			s2.quantite_recolte,
			s3.qte_semis,
			s3.qte_semis - sum(s2.quantite_recolte)over(partition by s2.saison order by s2.annee, s2.semaine rows between unbounded preceding and 1 preceding) as nbre_semis_restant
			from s2
			join s3 on s2.saison=s3.saison 
			order by s2.saison, s2.annee, s2.semaine), 
			b3 as(
				select 
				date_recolte,
				sum(quantite_recolte) as quantite_recolte 
				from cf_apports_pred
				group by 1 
				order by 1)
		select  -- final  : join entre b1 et b2
		cfa.date_recolte,
		b1.saison, 
		b1.semaine,
		b1.mi_week_quantite_recolte,
		b1.mi_recolte_1,
		b1.mi_recolte_2,
		b1.mi_recolte_3,
		b1.mi_recolte_4,
		b1.mi_recolte_5,
		b1.ratio_1,
		b1.ratio_2,
		b1.ratio_3,
		b1.ratio_4,
		b1.ratio_5,
		b2.nbre_semis_restant
		from b3 cfa 
		left join b1 on extract(week from cfa.date_recolte)=b1.semaine
		and extract(year from cfa.date_recolte)=b1.annee
		left join b2 on extract(week from cfa.date_recolte)=b2.semaine
		and extract(year from cfa.date_recolte)=b2.annee
		order by cfa.date_recolte;
	else
		create table apports_semis_agg as
		with b1 as(
			with sup_agg as(   -- lags de mi_recolte par semaine, cf, permettant des données pour les prédictions du jeudi
				with agg as( -- agg s1 à la semaine
					with s1 as( -- ajout saison apports, agg au jour, qté récolte mi_semaine
						select
						case 
						when date_recolte<'2016-09-01' then 2015
						when date_recolte between '2016-09-01' and '2017-08-31' then 2016
						when date_recolte between '2017-09-01' and '2018-08-31' then 2017
						when date_recolte between '2018-09-01' and '2019-08-31' then 2018
						when date_recolte between '2019-09-01' and '2020-08-31' then 2019
						when date_recolte between '2020-09-01' and '2021-08-31' then 2020
						when date_recolte between '2021-09-01' and '2022-08-31' then 2021
						when date_recolte >='2022-09-01' then 2022 end as saison,
						date_recolte,
						quantite_recolte 
						from cf_apports)
					select 
					sum(quantite_recolte) as quantite_recolte,
					date_recolte, 
					saison
					from s1
					where extract(dow from date_recolte)<=4
					group by 2, 3)
				select 
				extract(week from date_recolte) as semaine,
				extract(year from date_recolte) as annee,
				saison,
				sum(quantite_recolte) as mi_week_quantite_recolte
				from agg 
				group by 1, 2, 3),
			sup_agg2 as(  -- agg apports à la semaine
				select 
				sum(quantite_recolte) as quantite_recolte,
				extract(week from date_recolte) as semaine,
				extract(year from date_recolte) as annee
				from cf_apports
				group by 2, 3)
			select sup_agg.*,  -- feature engineering avec mi_recolte/semaine selon la saison
			sup_agg2.quantite_recolte,
			sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float as ratio,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 1)over(order by sup_agg.annee, sup_agg.semaine) as ratio_1,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 2)over(order by sup_agg.annee, sup_agg.semaine) as ratio_2,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 3)over(order by sup_agg.annee, sup_agg.semaine) as ratio_3,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 4)over(order by sup_agg.annee, sup_agg.semaine) as ratio_4,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 5)over(order by sup_agg.annee, sup_agg.semaine) as ratio_5,
			lag(sup_agg.mi_week_quantite_recolte, 1)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_1,
			lag(sup_agg.mi_week_quantite_recolte, 2)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_2,
			lag(sup_agg.mi_week_quantite_recolte, 3)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_3,
			lag(sup_agg.mi_week_quantite_recolte, 4)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_4,
			lag(sup_agg.mi_week_quantite_recolte, 5)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_5
			from sup_agg join sup_agg2
			on sup_agg.annee=sup_agg2.annee
			and sup_agg.semaine=sup_agg2.semaine 
			order by sup_agg.annee, sup_agg.semaine), 
		b2 as(
				with s2 as(   -- ajout saison aux apports
					select
					case 
					when date_recolte<'2016-09-01' then 2015
					when date_recolte between '2016-09-01' and '2017-08-31' then 2016
					when date_recolte between '2017-09-01' and '2018-08-31' then 2017
					when date_recolte between '2018-09-01' and '2019-08-31' then 2018
					when date_recolte between '2019-09-01' and '2020-08-31' then 2019
					when date_recolte between '2020-09-01' and '2021-08-31' then 2020
					when date_recolte between '2021-09-01' and '2022-08-31' then 2021
					when date_recolte >='2022-09-01' then 2022 end as saison,
					extract(week from date_recolte) as semaine,
					extract(year from date_recolte) as annee,
					sum(quantite_recolte) as quantite_recolte
					from cf_apports
					group by 1, 2, 3 order by 1, 3, 2), 
				s3 as(  -- calcul semis puis ajout saison
					with aggs as(  -- calcul semis par producteur par an TODO --> saison aggs !!
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2016 as qte_semis, 2016 as annee from cf_ds_semis cds where extract(year from date_plantation)=2016 and surface_2016 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2017 as qte_semis, 2017 as annee from cf_ds_semis cds where extract(year from date_plantation)=2017 and surface_2017 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2018 as qte_semis, 2018 as annee from cf_ds_semis cds where extract(year from date_plantation)=2018 and surface_2018 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2019 as qte_semis, 2019 as annee from cf_ds_semis cds where extract(year from date_plantation)=2019 and surface_2019 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2020 as qte_semis, 2020 as annee from cf_ds_semis cds where extract(year from date_plantation)=2020 and surface_2020 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2021 as qte_semis, 2021 as annee from cf_ds_semis cds where extract(year from date_plantation)=2021 and surface_2021 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2022 as qte_semis, 2022 as annee from cf_ds_semis cds where extract(year from date_plantation)=2022 and surface_2022 is not null
					)
				select
				case 
				when date_plantation<'2016-09-01' then 2015
				when date_plantation between '2015-05-15' and '2016-05-14' then 2016
				when date_plantation between '2016-05-15' and '2017-05-14' then 2017
				when date_plantation between '2017-05-15' and '2018-05-14' then 2018
				when date_plantation between '2018-05-15' and '2019-05-14' then 2019
				when date_plantation between '2019-05-15' and '2020-05-14' then 2020
				when date_plantation between '2020-05-15' and '2021-05-14' then 2021
				when date_plantation between '2021-05-15' and '2022-05-14' then 2022
				when date_plantation between '2023-05-15' and '2023-05-14' then 2023
				end as saison,
				sum((qte_semis)*3)::int as qte_semis
				from aggs
				group by 1
				)
			select -- calcul semis restant par rapport aux apports
			s2.saison,
			s2.semaine,
			s2.annee,
			s2.quantite_recolte,
			s3.qte_semis,
			s3.qte_semis - sum(s2.quantite_recolte)over(partition by s2.saison order by s2.annee, s2.semaine rows between unbounded preceding and 1 preceding) as nbre_semis_restant
			from s2
			join s3 on s2.saison=s3.saison 
			order by s2.saison, s2.annee, s2.semaine), 
			b3 as(
				select 
				date_recolte,
				sum(quantite_recolte) as quantite_recolte 
				from cf_apports 
				group by 1 
				order by 1)
		select  -- final  : join entre b1 et b2
		cfa.date_recolte,
		b1.saison, 
		b1.semaine,
		b1.mi_week_quantite_recolte,
		b1.mi_recolte_1,
		b1.mi_recolte_2,
		b1.mi_recolte_3,
		b1.mi_recolte_4,
		b1.mi_recolte_5,
		b1.ratio_1,
		b1.ratio_2,
		b1.ratio_3,
		b1.ratio_4,
		b1.ratio_5,
		b2.nbre_semis_restant
		from b3 cfa 
		left join b1 on extract(week from cfa.date_recolte)=b1.semaine
		and extract(year from cfa.date_recolte)=b1.annee
		left join b2 on extract(week from cfa.date_recolte)=b2.semaine
		and extract(year from cfa.date_recolte)=b2.annee
		order by cfa.date_recolte;
end if;
	
		create table cf_fe_apports_semis as 
		with agg as(
			select 
			cfa.date_recolte, 
			cfa.quantite_recolte,
			cfa.qte_semis,
			b1.saison, 
			b1.semaine,
			b1.mi_week_quantite_recolte,
			b1.mi_recolte_1,
			b1.mi_recolte_2,
			b1.mi_recolte_3,
			b1.mi_recolte_4,
			b1.mi_recolte_5,
			b1.ratio_1,
			b1.ratio_2,
			b1.ratio_3,
			b1.ratio_4,
			b1.ratio_5,
			b1.nbre_semis_restant,
			case when extract(doy from cfa.date_recolte)<244 then extract(doy from cfa.date_recolte)+365 
			else extract(doy from cfa.date_recolte) end as jour_saison
			from cf_apports_semis cfa 
			left join apports_semis_agg b1 
			on cfa.date_recolte=b1.date_recolte
			order by cfa.date_recolte)
			select date_recolte,quantite_recolte, qte_semis, saison, semaine,  jour_saison-244 as jour_saison from agg;
		
	

		create table truc23 as
			select 
			b1.date_recolte, 
			b1.jour_saison::int,
			b1.quantite_recolte::int,
			b1.qte_semis::int,
--			b1.saison::int, 
--			b1.semaine::int,
--			b1.mi_week_quantite_recolte::int,
--			b1.mi_recolte_1::int,
--			b1.mi_recolte_2::int,
--			b1.mi_recolte_3::int,
--			b1.mi_recolte_4::int,
--			b1.mi_recolte_5::int,
--			b1.ratio_1::float,
--			b1.ratio_2::float,
--			b1.ratio_3::float,
--			b1.ratio_4::float,
--			b1.ratio_5::float,
--			b1.nbre_semis_restant::int,
--			extract(month from meteo_cf2_corr.date_reelle)::int as mois_annee, 
			meteo_cf2_corr.tmin::float,
			meteo_cf2_corr.tmax::float,
			meteo_cf2_corr.rr10::float,
--			meteo_cf2_corr.t1::float,
--			meteo_cf2_corr.t2::float,
--			meteo_cf2_corr.t4::float,
--			meteo_cf2_corr.p1::float,
--			meteo_cf2_corr.p2::float,
--			meteo_cf2_corr.p4::float,
--			meteo_cf2_corr.p7::float,
--			meteo_cf2_corr.jourdoux::float,
--			meteo_cf2_corr.jourchaud::float,
--			meteo_cf2_corr.arrosage::float,
--			meteo_cf2_corr.cumultmax2::float,
--			meteo_cf2_corr.cumultmax3::float,
--			meteo_cf2_corr.cumultmax4::float,
--			meteo_cf2_corr.cumultmax5::float,
			meteo_cf2_corr.cumultmax7::float,
--			meteo_cf2_corr.cumultmax9::float,
--			meteo_cf2_corr.cumultmax13::float,
--			meteo_cf2_corr.cumultmax18::float,
--			meteo_cf2_corr.cumultmax22::float,
--			meteo_cf2_corr.cumultmax29::float,
			meteo_cf2_corr.cumultmax38::float,
			meteo_cf2_corr.cumultmax60::float,
--			meteo_cf2_corr.cumultmax90::float,
--			meteo_cf2_corr.cumultmax120::float,
--			meteo_cf2_corr.cumultmin2::float,
--			meteo_cf2_corr.cumultmin3::float,
--			meteo_cf2_corr.cumultmin4::float,
--			meteo_cf2_corr.cumultmin5::float,
			meteo_cf2_corr.cumultmin8::float,
--			meteo_cf2_corr.cumultmin12::float,
--			meteo_cf2_corr.cumultmin19::float,
--			meteo_cf2_corr.cumultmin25::float,
--			meteo_cf2_corr.cumultmin29::float,
			meteo_cf2_corr.cumultmin35::float,
			meteo_cf2_corr.cumultmin60::float,
--			meteo_cf2_corr.cumultmin90::float,
--			meteo_cf2_corr.cumultmin120::float,
--			meteo_cf2_corr.cumuljourfroid4::float,
			meteo_cf2_corr.cumuljourfroid7::float,
--			meteo_cf2_corr.cumuljourfroid13::float,
--			meteo_cf2_corr.cumuljourfroid18::float,
--			meteo_cf2_corr.cumuljourfroid28::float,
			meteo_cf2_corr.cumuljourfroid35::float,
			meteo_cf2_corr.cumuljourfroid60::float,
--			meteo_cf2_corr.cumuljourfroid90::float,
--			meteo_cf2_corr.cumuljourfroid120::float,
--			meteo_cf2_corr.cumuljourchaud2::float,
--			meteo_cf2_corr.cumuljourchaud4::float,
--			meteo_cf2_corr.cumuljourchaud6::float,
			meteo_cf2_corr.cumuljourchaud8::float,
--			meteo_cf2_corr.cumuljourchaud11::float,
--			meteo_cf2_corr.cumuljourchaud17::float,
--			meteo_cf2_corr.cumuljourchaud23::float,
--			meteo_cf2_corr.cumuljourchaud33::float,
			meteo_cf2_corr.cumuljourchaud38::float,
			meteo_cf2_corr.cumuljourchaud60::float,
--			meteo_cf2_corr.cumuljourchaud90::float,
--			meteo_cf2_corr.cumuljourchaud120::float,
--			meteo_cf2_corr.cumulrr102::float,
--			meteo_cf2_corr.cumulrr103::float,
--			meteo_cf2_corr.cumulrr104::float,
--			meteo_cf2_corr.cumulrr105::float,
			meteo_cf2_corr.cumulrr107::float,
--			meteo_cf2_corr.cumulrr1010::float,
--			meteo_cf2_corr.cumulrr1017::float,
--			meteo_cf2_corr.cumulrr1027::float,
			meteo_cf2_corr.cumulrr1037::float,
			meteo_cf2_corr.cumulrr1060::float,
--			meteo_cf2_corr.cumulrr1090::float,
--			meteo_cf2_corr.cumulrr10120::float,
--			meteo_cf2_corr.cumuljourdoux4::float,
			meteo_cf2_corr.cumuljourdoux9::float,
--			meteo_cf2_corr.cumuljourdoux13::float,
--			meteo_cf2_corr.cumuljourdoux15::float,
--			meteo_cf2_corr.cumuljourdoux26::float,
			meteo_cf2_corr.cumuljourdoux38::float,
			meteo_cf2_corr.cumuljourdoux60::float,
--			meteo_cf2_corr.cumuljourdoux90::float,
--			meteo_cf2_corr.cumuljourdoux120::float,
			meteo_cf2_corr.cumularrosage7::float,
--			meteo_cf2_corr.cumularrosage17::float,
--			meteo_cf2_corr.cumularrosage27::float,
			meteo_cf2_corr.cumularrosage37::float,
			meteo_cf2_corr.cumularrosage60::float,
--			meteo_cf2_corr.cumularrosage90::float,
--			meteo_cf2_corr.cumularrosage120::float
--			meteo_cf2_corr.cumulgel3::int,
--			meteo_cf2_corr.cumulgel4::int,
--			meteo_cf2_corr.cumulgel5::int,
--			meteo_cf2_corr.cumulgel6::int,
			meteo_cf2_corr.cumulgel7::int,
			meteo_cf2_corr.cumulgel38::int,
			meteo_cf2_corr.cumulgel60::int,
--			meteo_cf2_corr.cumuljtf3::int,
--			meteo_cf2_corr.cumuljtf4::int,
--			meteo_cf2_corr.cumuljtf5::int,
--			meteo_cf2_corr.cumuljtf6::int,
			meteo_cf2_corr.cumuljtf7::int,
			meteo_cf2_corr.cumuljtf38::int,
			meteo_cf2_corr.cumuljtf60::int,
--			meteo_cf2_corr.cumulpm3::int,
--			meteo_cf2_corr.cumulpm4::int,
--			meteo_cf2_corr.cumulpm5::int,
--			meteo_cf2_corr.cumulpm6::int,
			meteo_cf2_corr.cumulpm7::int,
			meteo_cf2_corr.cumulpm38::int,
			meteo_cf2_corr.cumulpm60::int,
--			meteo_cf2_corr.cumulpf3::int,
--			meteo_cf2_corr.cumulpf4::int,
--			meteo_cf2_corr.cumulpf5::int,
--			meteo_cf2_corr.cumulpf6::int,
			meteo_cf2_corr.cumulpf7::int,
			meteo_cf2_corr.cumulpf38::int,
			meteo_cf2_corr.cumulpf60::int,
			meteo_cf2_corr.cumulfroid7::int,
			meteo_cf2_corr.cumulfroid38::int,
			meteo_cf2_corr.cumulfroid60::int
			from cf_fe_apports_semis b1
			left join meteo_cf2_corr
			on b1.date_recolte=meteo_cf2_corr.date_reelle
			order by b1.date_recolte;
	
--			delete from truc23 where mi_recolte_5 is null;
		
		
			if $1='predict' then
				return query select * from truc23 where date_recolte>$2+2 and date_recolte<=$2+10 order by date_recolte;
			else
				return query select * from truc23 where date_recolte>'2016-03-04';
			end if;
			
		
			drop table if exists cf_apports_semis;
			drop table if exists meteo_cf2_corr;
			drop table if exists truc23;
			drop table if exists s1;
			drop table if exists cf_fe_apports_semis;
			drop table if exists apports_semis_agg;
			drop table if exists cf_semis_saison;	
			drop table if exists cf_apports_saison;

	
END;
$function$
;




CREATE OR REPLACE FUNCTION public.cf3_test(_max_date date)
 RETURNS TABLE(liv_date date, jr_saison integer, qte_recolte integer, quantite_initiale integer, t_min double precision, t_max double precision, pluie double precision, cumul_tmax7 double precision, cumul_tmax38 double precision, cumul_tmax60 double precision, cumul_tmin8 double precision, cumul_tmin35 double precision, cumul_tmin60 double precision, cumul_jourfroid7 double precision, cumul_jourfroid35 double precision, cumul_jourfroid60 double precision, cumul_jourchaud8 double precision, cumul_jourchaud38 double precision, cumul_jourchaud60 double precision, cumul_pluie7 double precision, cumul_pluie37 double precision, cumul_pluie60 double precision, cumul_jourdoux9 double precision, cumul_jourdoux38 double precision, cumul_jourdoux60 double precision, cumul_jour_de_pluie7 double precision, cumul_jour_de_pluie37 double precision, cumul_jour_de_pluie60 double precision, gel7 integer, gel38 integer, gel60 integer, jtf7 integer, jtf38 integer, jtf60 integer, pm7 integer, pm38 integer, pm60 integer, pf7 integer, pf38 integer, pf60 integer, fr7 integer, fr38 integer, df60 integer)
 LANGUAGE plpgsql
AS $function$
	begin 
	
			drop table if exists cf_apports_semis;
			drop table if exists meteo_cf2_corr;
			drop table if exists truc23;
			drop table if exists s1;
			drop table if exists cf_fe_apports_semis;
			drop table if exists apports_semis_agg;
			drop table if exists cf_semis_saison;	
			drop table if exists cf_apports_saison;
		
		create table meteo_cf2_corr as
		with agg as(
			select
			meteo.*,
			CASE WHEN tmin<0 THEN 1 ELSE 0 END AS gel,
			CASE WHEN tmin<5 THEN 1 ELSE 0 END AS froid,
			case when tmax<10 then 1 else 0 end as jourfroid,
			CASE WHEN tmax<5 THEN 1 ELSE 0 END AS jourtresfroid,
			CASE WHEN (tmax>=15 AND tmax<=25) THEN 1 ELSE 0 END AS jourchaud,
			CASE WHEN (tmax>=15 AND tmax<=20) THEN 1 ELSE 0 END AS jourdoux,
			CASE WHEN (rr10>0) THEN 1 ELSE 0 END AS arrosage,
			CASE WHEN (rr10>8) THEN 1 ELSE 0 END AS pluiemoderee,
			CASE WHEN (rr10>15) THEN 1 ELSE 0 END AS pluieforte
			from meteo
			where code_insee=29030
		)
		select
		date_reelle,
		tmax,
		tmin,
		rr10,
		jourchaud,
		jourdoux,
		arrosage,
		lag(tmax, 1) over (order by code_insee, date_reelle) as t1,
		lag(tmax, 2) over (order by code_insee, date_reelle) as t2,
		lag(tmax, 4) over (order by code_insee, date_reelle) as t4,
		lag(rr10, 1) over (order by code_insee, date_reelle) as p1,
		lag(rr10, 2) over (order by code_insee, date_reelle) as p2,
		lag(rr10, 4) over (order by code_insee, date_reelle) as p4,
		lag(rr10, 7) over (order by code_insee, date_reelle) as p7,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumultmax2,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumultmax3,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumultmax4,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumultmax5,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumultmax7,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 10 preceding and 1 preceding) as cumultmax9,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 14 preceding and 1 preceding) as cumultmax13,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 19 preceding and 1 preceding) as cumultmax18,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 23 preceding and 1 preceding) as cumultmax22,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 30 preceding and 1 preceding) as cumultmax29,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumultmax38,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumultmax60,
--		sum(tmax) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumultmax90,
--		sum(tmax) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumultmax120,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumultmin2,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumultmin3,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumultmin4,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumultmin5,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 9 preceding and 1 preceding) as cumultmin8,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 13 preceding and 1 preceding) as cumultmin12,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 20 preceding and 1 preceding) as cumultmin19,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 26 preceding and 1 preceding) as cumultmin25,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 30 preceding and 1 preceding) as cumultmin29,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 36 preceding and 1 preceding) as cumultmin35,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumultmin60,
--		sum(tmin) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumultmin90,
--		sum(tmin) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumultmin120,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumulrr102,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulrr103,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulrr104,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulrr105,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulrr107,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 11 preceding and 1 preceding) as cumulrr1010,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 18 preceding and 1 preceding) as cumulrr1017,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 28 preceding and 1 preceding) as cumulrr1027,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 38 preceding and 1 preceding) as cumulrr1037,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulrr1060,
--		sum(rr10) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumulrr1090,
--		sum(rr10) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumulrr10120,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljourfroid4,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumuljourfroid7,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 14 preceding and 1 preceding) as cumuljourfroid13,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 19 preceding and 1 preceding) as cumuljourfroid18,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 29 preceding and 1 preceding) as cumuljourfroid28,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 36 preceding and 1 preceding) as cumuljourfroid35,
		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljourfroid60,
--		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumuljourfroid90,
--		sum(jourfroid) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumuljourfroid120,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 3 preceding and 1 preceding) as cumuljourchaud2,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljourchaud4,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumuljourchaud6,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 9 preceding and 1 preceding) as cumuljourchaud8,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 12 preceding and 1 preceding) as cumuljourchaud11,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 18 preceding and 1 preceding) as cumuljourchaud17,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 24 preceding and 1 preceding) as cumuljourchaud23,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 34 preceding and 1 preceding) as cumuljourchaud33,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumuljourchaud38,
		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljourchaud60,
--		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumuljourchaud90,
--		sum(jourchaud) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumuljourchaud120,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljourdoux4,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 10 preceding and 1 preceding) as cumuljourdoux9,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 14 preceding and 1 preceding) as cumuljourdoux13,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 16 preceding and 1 preceding) as cumuljourdoux15,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 27 preceding and 1 preceding) as cumuljourdoux26,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumuljourdoux38,
		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljourdoux60,
--		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumuljourdoux90,
--		sum(jourdoux) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumuljourdoux120,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumularrosage7,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 18 preceding and 1 preceding) as cumularrosage17,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 28 preceding and 1 preceding) as cumularrosage27,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 38 preceding and 1 preceding) as cumularrosage37,
		sum(arrosage) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumularrosage60,
--		sum(arrosage) over(partition by code_insee order by date_reelle rows between 91 preceding and 1 preceding) as cumularrosage90,
--		sum(arrosage) over(partition by code_insee order by date_reelle rows between 121 preceding and 1 preceding) as cumularrosage120
		sum(gel) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulgel3,
		sum(gel) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulgel4,
		sum(gel) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulgel5,
		sum(gel) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulgel6,
		sum(gel) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulgel7,
		sum(gel) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulgel38,
		sum(gel) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulgel60,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumuljtf3,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumuljtf4,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumuljtf5,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumuljtf6,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumuljtf7,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumuljtf38,
		sum(jourtresfroid) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumuljtf60,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulpm3,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulpm4,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulpm5,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulpm6,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulpm7,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulpm38,
		sum(pluiemoderee) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulpm60,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulpf3,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulpf4,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulpf5,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulpf6,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulpf7,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulpf38,
		sum(pluieforte) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulpf60,
		sum(froid) over(partition by code_insee order by date_reelle rows between 4 preceding and 1 preceding) as cumulfroid3,
		sum(froid) over(partition by code_insee order by date_reelle rows between 5 preceding and 1 preceding) as cumulfroid4,
		sum(froid) over(partition by code_insee order by date_reelle rows between 6 preceding and 1 preceding) as cumulfroid5,
		sum(froid) over(partition by code_insee order by date_reelle rows between 7 preceding and 1 preceding) as cumulfroid6,
		sum(froid) over(partition by code_insee order by date_reelle rows between 8 preceding and 1 preceding) as cumulfroid7,
		sum(froid) over(partition by code_insee order by date_reelle rows between 39 preceding and 1 preceding) as cumulfroid38,
		sum(froid) over(partition by code_insee order by date_reelle rows between 61 preceding and 1 preceding) as cumulfroid60
		from agg;
	
	
		create table s1 as
		with agg as(
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2016 as qte_semis, 2016 as annee from cf_ds_semis cds where extract(year from date_plantation)=2016 and surface_2016 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2017 as qte_semis, 2017 as annee from cf_ds_semis cds where extract(year from date_plantation)=2017 and surface_2017 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2018 as qte_semis, 2018 as annee from cf_ds_semis cds where extract(year from date_plantation)=2018 and surface_2018 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2019 as qte_semis, 2019 as annee from cf_ds_semis cds where extract(year from date_plantation)=2019 and surface_2019 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2020 as qte_semis, 2020 as annee from cf_ds_semis cds where extract(year from date_plantation)=2020 and surface_2020 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2021 as qte_semis, 2021 as annee from cf_ds_semis cds where extract(year from date_plantation)=2021 and surface_2021 is not null
		union all
		select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2022 as qte_semis, 2022 as annee from cf_ds_semis cds where extract(year from date_plantation)=2022 and surface_2022 is not null
		)
		select * from agg;

		create table cf_semis_saison as
		select s1.*,
		case
		when date_plantation<'2016-09-01' then 2015
		when date_plantation between '2015-05-15' and '2016-05-14' then 2016
		when date_plantation between '2016-05-15' and '2017-05-14' then 2017
		when date_plantation between '2017-05-15' and '2018-05-14' then 2018
		when date_plantation between '2018-05-15' and '2019-05-14' then 2019
		when date_plantation between '2019-05-15' and '2020-05-14' then 2020
		when date_plantation between '2020-05-15' and '2021-05-14' then 2021
		when date_plantation between '2021-05-15' and '2022-05-14' then 2022
		when date_plantation between '2023-05-15' and '2023-05-14' then 2023
		end as saison
		from s1;
	
		
			create table cf_apports_saison as 
			with agg as(
			select cf_apports.*,
			case 
			when date_recolte<'2016-09-01' then 2015
			when date_recolte between '2016-09-01' and '2017-08-31' then 2016
			when date_recolte between '2017-09-01' and '2018-08-31' then 2017
			when date_recolte between '2018-09-01' and '2019-08-31' then 2018
			when date_recolte between '2019-09-01' and '2020-08-31' then 2019
			when date_recolte between '2020-09-01' and '2021-08-31' then 2020
			when date_recolte between '2021-09-01' and '2022-08-31' then 2021
			when date_recolte between '2022-09-01' and '2023-08-31' then 2022
			when date_recolte between '2023-09-01' and '2024-08-31' then 2023
			end as saison
			from cf_apports)
			select 
			agg.date_recolte,
			agg.saison,
			agg.code_adherent,
			sum(agg.quantite_recolte) as quantite_recolte
			from agg
			group by 1, 2, 3
			order by 1;
	
		
		create table cf_apports_semis as
			with sup_agg as(
				with agg as( 
					select 
					code_adherent, 
					saison, 
					sum(qte_semis) as qte_semis
					from cf_semis_saison
					group by 1, 2
					order by 2)
				select 
				cfa.code_adherent,
				cfa.date_recolte,
				cfa.quantite_recolte,
				agg.qte_semis
				from cf_apports_saison cfa 
				left join agg
				on agg.saison=cfa.saison
				and cfa.code_adherent=agg.code_adherent
				order by 2)
			select 
			date_recolte, 
			sum(qte_semis) as qte_semis,
			sum(quantite_recolte) as quantite_recolte
			from sup_agg
			group by 1;
		
	
		
		
		create table apports_semis_agg as
		with b1 as(
			with sup_agg as(   -- lags de mi_recolte par semaine, cf, permettant des données pour les prédictions du jeudi
				with agg as( -- agg s1 à la semaine
					with s1 as( -- ajout saison apports, agg au jour, qté récolte mi_semaine
						select
						case 
						when date_recolte<'2016-09-01' then 2015
						when date_recolte between '2016-09-01' and '2017-08-31' then 2016
						when date_recolte between '2017-09-01' and '2018-08-31' then 2017
						when date_recolte between '2018-09-01' and '2019-08-31' then 2018
						when date_recolte between '2019-09-01' and '2020-08-31' then 2019
						when date_recolte between '2020-09-01' and '2021-08-31' then 2020
						when date_recolte between '2021-09-01' and '2022-08-31' then 2021
						when date_recolte >='2022-09-01' then 2022 end as saison,
						date_recolte,
						quantite_recolte 
						from cf_apports)
					select 
					sum(quantite_recolte) as quantite_recolte,
					date_recolte, 
					saison
					from s1
					where extract(dow from date_recolte)<=4
					group by 2, 3)
				select 
				extract(week from date_recolte) as semaine,
				extract(year from date_recolte) as annee,
				saison,
				sum(quantite_recolte) as mi_week_quantite_recolte
				from agg 
				group by 1, 2, 3),
			sup_agg2 as(  -- agg apports à la semaine
				select 
				sum(quantite_recolte) as quantite_recolte,
				extract(week from date_recolte) as semaine,
				extract(year from date_recolte) as annee
				from cf_apports
				group by 2, 3)
			select sup_agg.*,  -- feature engineering avec mi_recolte/semaine selon la saison
			sup_agg2.quantite_recolte,
			sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float as ratio,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 1)over(order by sup_agg.annee, sup_agg.semaine) as ratio_1,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 2)over(order by sup_agg.annee, sup_agg.semaine) as ratio_2,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 3)over(order by sup_agg.annee, sup_agg.semaine) as ratio_3,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 4)over(order by sup_agg.annee, sup_agg.semaine) as ratio_4,
			lag((sup_agg.mi_week_quantite_recolte::float/sup_agg2.quantite_recolte::float), 5)over(order by sup_agg.annee, sup_agg.semaine) as ratio_5,
			lag(sup_agg.mi_week_quantite_recolte, 1)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_1,
			lag(sup_agg.mi_week_quantite_recolte, 2)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_2,
			lag(sup_agg.mi_week_quantite_recolte, 3)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_3,
			lag(sup_agg.mi_week_quantite_recolte, 4)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_4,
			lag(sup_agg.mi_week_quantite_recolte, 5)over(order by sup_agg.annee, sup_agg.semaine) as mi_recolte_5
			from sup_agg join sup_agg2
			on sup_agg.annee=sup_agg2.annee
			and sup_agg.semaine=sup_agg2.semaine 
			order by sup_agg.annee, sup_agg.semaine), 
		b2 as(
				with s2 as(   -- ajout saison aux apports
					select
					case 
					when date_recolte<'2016-09-01' then 2015
					when date_recolte between '2016-09-01' and '2017-08-31' then 2016
					when date_recolte between '2017-09-01' and '2018-08-31' then 2017
					when date_recolte between '2018-09-01' and '2019-08-31' then 2018
					when date_recolte between '2019-09-01' and '2020-08-31' then 2019
					when date_recolte between '2020-09-01' and '2021-08-31' then 2020
					when date_recolte between '2021-09-01' and '2022-08-31' then 2021
					when date_recolte >='2022-09-01' then 2022 end as saison,
					extract(week from date_recolte) as semaine,
					extract(year from date_recolte) as annee,
					sum(quantite_recolte) as quantite_recolte
					from cf_apports
					group by 1, 2, 3 order by 1, 3, 2), 
				s3 as(  -- calcul semis puis ajout saison
					with aggs as(  -- calcul semis par producteur par an TODO --> saison aggs !!
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2016 as qte_semis, 2016 as annee from cf_ds_semis cds where extract(year from date_plantation)=2016 and surface_2016 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2017 as qte_semis, 2017 as annee from cf_ds_semis cds where extract(year from date_plantation)=2017 and surface_2017 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2018 as qte_semis, 2018 as annee from cf_ds_semis cds where extract(year from date_plantation)=2018 and surface_2018 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2019 as qte_semis, 2019 as annee from cf_ds_semis cds where extract(year from date_plantation)=2019 and surface_2019 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2020 as qte_semis, 2020 as annee from cf_ds_semis cds where extract(year from date_plantation)=2020 and surface_2020 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2021 as qte_semis, 2021 as annee from cf_ds_semis cds where extract(year from date_plantation)=2021 and surface_2021 is not null
					union all
					select code_adherent::varchar(50), variete_norm, date_plantation, 12000*surface_2022 as qte_semis, 2022 as annee from cf_ds_semis cds where extract(year from date_plantation)=2022 and surface_2022 is not null
					)
				select
				case 
				when date_plantation<'2016-09-01' then 2015
				when date_plantation between '2015-05-15' and '2016-05-14' then 2016
				when date_plantation between '2016-05-15' and '2017-05-14' then 2017
				when date_plantation between '2017-05-15' and '2018-05-14' then 2018
				when date_plantation between '2018-05-15' and '2019-05-14' then 2019
				when date_plantation between '2019-05-15' and '2020-05-14' then 2020
				when date_plantation between '2020-05-15' and '2021-05-14' then 2021
				when date_plantation between '2021-05-15' and '2022-05-14' then 2022
				when date_plantation between '2023-05-15' and '2023-05-14' then 2023
				end as saison,
				sum((qte_semis)*3)::int as qte_semis
				from aggs
				group by 1
				)
			select -- calcul semis restant par rapport aux apports
			s2.saison,
			s2.semaine,
			s2.annee,
			s2.quantite_recolte,
			s3.qte_semis,
			s3.qte_semis - sum(s2.quantite_recolte)over(partition by s2.saison order by s2.annee, s2.semaine rows between unbounded preceding and 1 preceding) as nbre_semis_restant
			from s2
			join s3 on s2.saison=s3.saison 
			order by s2.saison, s2.annee, s2.semaine), 
			b3 as(
				select 
				date_recolte,
				sum(quantite_recolte) as quantite_recolte 
				from cf_apports 
				group by 1 
				order by 1)
		select  -- final  : join entre b1 et b2
		cfa.date_recolte,
		b1.saison, 
		b1.semaine,
		b1.mi_week_quantite_recolte,
		b1.mi_recolte_1,
		b1.mi_recolte_2,
		b1.mi_recolte_3,
		b1.mi_recolte_4,
		b1.mi_recolte_5,
		b1.ratio_1,
		b1.ratio_2,
		b1.ratio_3,
		b1.ratio_4,
		b1.ratio_5,
		b2.nbre_semis_restant
		from b3 cfa 
		left join b1 on extract(week from cfa.date_recolte)=b1.semaine
		and extract(year from cfa.date_recolte)=b1.annee
		left join b2 on extract(week from cfa.date_recolte)=b2.semaine
		and extract(year from cfa.date_recolte)=b2.annee
		order by cfa.date_recolte;

	
		create table cf_fe_apports_semis as 
		with agg as(
			select 
			cfa.date_recolte, 
			cfa.quantite_recolte,
			cfa.qte_semis,
			b1.saison, 
			b1.semaine,
			b1.mi_week_quantite_recolte,
			b1.mi_recolte_1,
			b1.mi_recolte_2,
			b1.mi_recolte_3,
			b1.mi_recolte_4,
			b1.mi_recolte_5,
			b1.ratio_1,
			b1.ratio_2,
			b1.ratio_3,
			b1.ratio_4,
			b1.ratio_5,
			b1.nbre_semis_restant,
			case when extract(doy from cfa.date_recolte)<244 then extract(doy from cfa.date_recolte)+365 
			else extract(doy from cfa.date_recolte) end as jour_saison
			from cf_apports_semis cfa 
			left join apports_semis_agg b1 
			on cfa.date_recolte=b1.date_recolte
			where cfa.date_recolte>'2016-01-01' and cfa.date_recolte<$1
			order by cfa.date_recolte)
			select date_recolte,quantite_recolte, qte_semis, saison, semaine,  jour_saison-244 as jour_saison from agg;
		
	

		create table truc23 as
			select 
			b1.date_recolte, 
			b1.jour_saison::int,
			b1.quantite_recolte::int,
			b1.qte_semis::int,
--			b1.saison::int, 
--			b1.semaine::int,
--			b1.mi_week_quantite_recolte::int,
--			b1.mi_recolte_1::int,
--			b1.mi_recolte_2::int,
--			b1.mi_recolte_3::int,
--			b1.mi_recolte_4::int,
--			b1.mi_recolte_5::int,
--			b1.ratio_1::float,
--			b1.ratio_2::float,
--			b1.ratio_3::float,
--			b1.ratio_4::float,
--			b1.ratio_5::float,
--			b1.nbre_semis_restant::int,
--			extract(month from meteo_cf2_corr.date_reelle)::int as mois_annee, 
			meteo_cf2_corr.tmin::float,
			meteo_cf2_corr.tmax::float,
			meteo_cf2_corr.rr10::float,
--			meteo_cf2_corr.t1::float,
--			meteo_cf2_corr.t2::float,
--			meteo_cf2_corr.t4::float,
--			meteo_cf2_corr.p1::float,
--			meteo_cf2_corr.p2::float,
--			meteo_cf2_corr.p4::float,
--			meteo_cf2_corr.p7::float,
--			meteo_cf2_corr.jourdoux::float,
--			meteo_cf2_corr.jourchaud::float,
--			meteo_cf2_corr.arrosage::float,
--			meteo_cf2_corr.cumultmax2::float,
--			meteo_cf2_corr.cumultmax3::float,
--			meteo_cf2_corr.cumultmax4::float,
--			meteo_cf2_corr.cumultmax5::float,
			meteo_cf2_corr.cumultmax7::float,
--			meteo_cf2_corr.cumultmax9::float,
--			meteo_cf2_corr.cumultmax13::float,
--			meteo_cf2_corr.cumultmax18::float,
--			meteo_cf2_corr.cumultmax22::float,
--			meteo_cf2_corr.cumultmax29::float,
			meteo_cf2_corr.cumultmax38::float,
			meteo_cf2_corr.cumultmax60::float,
--			meteo_cf2_corr.cumultmax90::float,
--			meteo_cf2_corr.cumultmax120::float,
--			meteo_cf2_corr.cumultmin2::float,
--			meteo_cf2_corr.cumultmin3::float,
--			meteo_cf2_corr.cumultmin4::float,
--			meteo_cf2_corr.cumultmin5::float,
			meteo_cf2_corr.cumultmin8::float,
--			meteo_cf2_corr.cumultmin12::float,
--			meteo_cf2_corr.cumultmin19::float,
--			meteo_cf2_corr.cumultmin25::float,
--			meteo_cf2_corr.cumultmin29::float,
			meteo_cf2_corr.cumultmin35::float,
			meteo_cf2_corr.cumultmin60::float,
--			meteo_cf2_corr.cumultmin90::float,
--			meteo_cf2_corr.cumultmin120::float,
--			meteo_cf2_corr.cumuljourfroid4::float,
			meteo_cf2_corr.cumuljourfroid7::float,
--			meteo_cf2_corr.cumuljourfroid13::float,
--			meteo_cf2_corr.cumuljourfroid18::float,
--			meteo_cf2_corr.cumuljourfroid28::float,
			meteo_cf2_corr.cumuljourfroid35::float,
			meteo_cf2_corr.cumuljourfroid60::float,
--			meteo_cf2_corr.cumuljourfroid90::float,
--			meteo_cf2_corr.cumuljourfroid120::float,
--			meteo_cf2_corr.cumuljourchaud2::float,
--			meteo_cf2_corr.cumuljourchaud4::float,
--			meteo_cf2_corr.cumuljourchaud6::float,
			meteo_cf2_corr.cumuljourchaud8::float,
--			meteo_cf2_corr.cumuljourchaud11::float,
--			meteo_cf2_corr.cumuljourchaud17::float,
--			meteo_cf2_corr.cumuljourchaud23::float,
--			meteo_cf2_corr.cumuljourchaud33::float,
			meteo_cf2_corr.cumuljourchaud38::float,
			meteo_cf2_corr.cumuljourchaud60::float,
--			meteo_cf2_corr.cumuljourchaud90::float,
--			meteo_cf2_corr.cumuljourchaud120::float,
--			meteo_cf2_corr.cumulrr102::float,
--			meteo_cf2_corr.cumulrr103::float,
--			meteo_cf2_corr.cumulrr104::float,
--			meteo_cf2_corr.cumulrr105::float,
			meteo_cf2_corr.cumulrr107::float,
--			meteo_cf2_corr.cumulrr1010::float,
--			meteo_cf2_corr.cumulrr1017::float,
--			meteo_cf2_corr.cumulrr1027::float,
			meteo_cf2_corr.cumulrr1037::float,
			meteo_cf2_corr.cumulrr1060::float,
--			meteo_cf2_corr.cumulrr1090::float,
--			meteo_cf2_corr.cumulrr10120::float,
--			meteo_cf2_corr.cumuljourdoux4::float,
			meteo_cf2_corr.cumuljourdoux9::float,
--			meteo_cf2_corr.cumuljourdoux13::float,
--			meteo_cf2_corr.cumuljourdoux15::float,
--			meteo_cf2_corr.cumuljourdoux26::float,
			meteo_cf2_corr.cumuljourdoux38::float,
			meteo_cf2_corr.cumuljourdoux60::float,
--			meteo_cf2_corr.cumuljourdoux90::float,
--			meteo_cf2_corr.cumuljourdoux120::float,
			meteo_cf2_corr.cumularrosage7::float,
--			meteo_cf2_corr.cumularrosage17::float,
--			meteo_cf2_corr.cumularrosage27::float,
			meteo_cf2_corr.cumularrosage37::float,
			meteo_cf2_corr.cumularrosage60::float,
--			meteo_cf2_corr.cumularrosage90::float,
--			meteo_cf2_corr.cumularrosage120::float
--			meteo_cf2_corr.cumulgel3::int,
--			meteo_cf2_corr.cumulgel4::int,
--			meteo_cf2_corr.cumulgel5::int,
--			meteo_cf2_corr.cumulgel6::int,
			meteo_cf2_corr.cumulgel7::int,
			meteo_cf2_corr.cumulgel38::int,
			meteo_cf2_corr.cumulgel60::int,
--			meteo_cf2_corr.cumuljtf3::int,
--			meteo_cf2_corr.cumuljtf4::int,
--			meteo_cf2_corr.cumuljtf5::int,
--			meteo_cf2_corr.cumuljtf6::int,
			meteo_cf2_corr.cumuljtf7::int,
			meteo_cf2_corr.cumuljtf38::int,
			meteo_cf2_corr.cumuljtf60::int,
--			meteo_cf2_corr.cumulpm3::int,
--			meteo_cf2_corr.cumulpm4::int,
--			meteo_cf2_corr.cumulpm5::int,
--			meteo_cf2_corr.cumulpm6::int,
			meteo_cf2_corr.cumulpm7::int,
			meteo_cf2_corr.cumulpm38::int,
			meteo_cf2_corr.cumulpm60::int,
--			meteo_cf2_corr.cumulpf3::int,
--			meteo_cf2_corr.cumulpf4::int,
--			meteo_cf2_corr.cumulpf5::int,
--			meteo_cf2_corr.cumulpf6::int,
			meteo_cf2_corr.cumulpf7::int,
			meteo_cf2_corr.cumulpf38::int,
			meteo_cf2_corr.cumulpf60::int,
			meteo_cf2_corr.cumulfroid7::int,
			meteo_cf2_corr.cumulfroid38::int,
			meteo_cf2_corr.cumulfroid60::int
			from cf_fe_apports_semis b1
			left join meteo_cf2_corr
			on b1.date_recolte=meteo_cf2_corr.date_reelle
			order by b1.date_recolte;
	
--			delete from truc23 where mi_recolte_5 is null;
		
		
		
				return query select * from truc23 where date_recolte>'2016-03-04';
			
		
			drop table if exists cf_apports_semis;
			drop table if exists meteo_cf2_corr;
			drop table if exists truc23;
			drop table if exists s1;
			drop table if exists cf_fe_apports_semis;
			drop table if exists apports_semis_agg;
			drop table if exists cf_semis_saison;	
			drop table if exists cf_apports_saison;

	
END;
$function$
;



























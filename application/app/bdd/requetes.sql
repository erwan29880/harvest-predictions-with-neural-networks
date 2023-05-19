-- fonction flux brocolis 
create or replace function flux_brocolis(_flux_train VARCHAR(10), _full_dataset int)
returns table(
out_numero_station int, 
out_date_recolte date, 
out_tmax_30j_sum float, 
out_tmax_10j_sum float, 
out_tmax_2j_sum float, 
out_tmax float, 
out_rr10 float, 
out_rr_17_sum float, 
out_levee_brocolis_ideale float, 
out_jour_froid_sum float, 
out_quantite_initiale int, 
out_quantite_recolte int
)
as $$
begin
	
	drop table if exists c1;
	drop table if exists c2;
	drop table if exists c3;
	drop table if exists c4;
	drop table if exists c5;
	drop table if exists c6;
	drop table if exists c7;	
	
	if $1='train' then
	
		if $2=2016 then
			create table c1 as select
			bro_apports.prdcode as code_adherent,
			bro_apports.liv_qty as quantite_recolte, 
			bro_apports.liv_date as date_recolte, 
			EXTRACT(YEAR FROM bro_apports.liv_date) as annee,
			bro_producteur_station.code_insee 
			from bro_apports
			left join bro_producteur_station on bro_producteur_station.code_adherent=bro_apports.prdcode;
		else
			create table c1 as select
			bro_apports.prdcode as code_adherent,
			bro_apports.liv_qty as quantite_recolte, 
			bro_apports.liv_date as date_recolte, 
			EXTRACT(YEAR FROM bro_apports.liv_date) as annee,
			bro_producteur_station.code_insee 
			from bro_apports
			left join bro_producteur_station on bro_producteur_station.code_adherent=bro_apports.prdcode
			where liv_date>='2019-01-01';
		end if;
	else 
		create table c1 as select
		bro_apports_pred.prdcode as code_adherent,
		bro_apports_pred.liv_qty as quantite_recolte, 
		bro_apports_pred.liv_date as date_recolte, 
		EXTRACT(YEAR FROM bro_apports_pred.liv_date) as annee,
		bro_producteur_station.code_insee 
		from bro_apports_pred
		left join bro_producteur_station on bro_producteur_station.code_adherent=bro_apports_pred.prdcode
		where liv_date>'2022-01-01';
	end if;
	
	if $2=2016 then
		CREATE TABLE c2 AS(
			WITH agg AS(
			SELECT 
			date_reelle,
			tmin, 
			tmax,
			rr10,
			code_insee,
			EXTRACT(YEAR FROM date_reelle) as annee,
			CASE WHEN tmin<5 THEN 1 ELSE 0 END AS jour_froid,
			CASE WHEN (tmax>=15 AND tmax<=25) THEN 1 ELSE 0 END AS levee_fleur
			FROM meteo
			ORDER BY 1,2)
		SELECT
		agg.date_reelle,
		agg.code_insee,
		agg.tmin, 
		agg.tmax,
		rr10,
		SUM(agg.levee_fleur) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 11 PRECEDING and 1 PRECEDING) AS levee_brocolis_ideale,
		SUM(agg.tmax) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 31 PRECEDING and 1 PRECEDING) AS tmax_30j_sum,
		SUM(agg.tmax) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 11 PRECEDING and 1 PRECEDING) AS tmax_10j_sum,
		SUM(agg.tmax) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS tmax_2j_sum,
		SUM(agg.rr10) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 19 PRECEDING and 1 PRECEDING) AS rr_17j_sum,
		SUM(agg.jour_froid) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 20 PRECEDING and 1 PRECEDING) AS jour_froid_sum
		FROM agg
		ORDER BY 1,2);
	else
		CREATE TABLE c2 AS(
			WITH agg AS(
			SELECT 
			date_reelle,
			tmin, 
			tmax,
			rr10,
			code_insee,
			EXTRACT(YEAR FROM date_reelle) as annee,
			CASE WHEN tmin<5 THEN 1 ELSE 0 END AS jour_froid,
			CASE WHEN (tmax>=15 AND tmax<=25) THEN 1 ELSE 0 END AS levee_fleur
			FROM meteo
			where date_reelle>'2019-01-01'
			ORDER BY 1,2)
		SELECT
		agg.date_reelle,
		agg.code_insee,
		agg.tmin, 
		agg.tmax,
		rr10,
		SUM(agg.levee_fleur) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 11 PRECEDING and 1 PRECEDING) AS levee_brocolis_ideale,
		SUM(agg.tmax) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 31 PRECEDING and 1 PRECEDING) AS tmax_30j_sum,
		SUM(agg.tmax) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 11 PRECEDING and 1 PRECEDING) AS tmax_10j_sum,
		SUM(agg.tmax) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 3 PRECEDING and 1 PRECEDING) AS tmax_2j_sum,
		SUM(agg.rr10) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 19 PRECEDING and 1 PRECEDING) AS rr_17j_sum,
		SUM(agg.jour_froid) OVER (PARTITION BY agg.annee, agg.code_insee ORDER BY agg.date_reelle ASC ROWS BETWEEN 20 PRECEDING and 1 PRECEDING) AS jour_froid_sum
		FROM agg
		ORDER BY 1,2);
	end if;
	
	
	if $2=2016 then
		create table c3 as(
			with agg as(
			select bro_semis.quantite_initiale,
			bro_semis.annee,
			bro_producteur_station.code_adherent,
			bro_producteur_station.code_insee
			from bro_semis
			left join bro_producteur_station
			on bro_producteur_station.code_adherent=bro_semis.code_adherent)
		select sum(agg.quantite_initiale) as quantite_initiale,
		agg.code_insee,
		agg.annee
		from agg
		group by 2,3
		order by 2);
	else
		create table c3 as(
			with agg as(
			select bro_semis.quantite_initiale,
			bro_semis.annee,
			bro_producteur_station.code_adherent,
			bro_producteur_station.code_insee
			from bro_semis
			left join bro_producteur_station
			on bro_producteur_station.code_adherent=bro_semis.code_adherent
			where bro_semis.annee>=2019)
		select sum(agg.quantite_initiale) as quantite_initiale,
		agg.code_insee,
		agg.annee
		from agg
		group by 2,3
		order by 2);
	end if;	
	
	
	CREATE TABLE c4 as(
		with agg as(
		select code_insee,
		date_recolte,
		sum(quantite_recolte) as quantite_recolte,
		EXTRACT(YEAR FROM date_recolte) as annee
		from c1 
		group by 1,2,4 
		order by 2)
	select agg.*,
	c3.quantite_initiale
	from agg
	left join c3 
	on c3.code_insee=agg.code_insee
	and c3.annee=agg.annee);

	CREATE table c5 as(
		with agg as(
		select c4.*
		from c4
		group by 1,2,3,4,5
		order by 1,2)
	select agg.*,
	c2.tmin, 
	c2.tmax,
	c2.rr10,
	c2.levee_brocolis_ideale,
	c2.tmax_30j_sum,
	c2.tmax_10j_sum,
	c2.tmax_2j_sum,
	c2.jour_froid_sum,
	c2.rr_17j_sum
	from agg
	left join c2
	on c2.code_insee=agg.code_insee
	and c2.date_reelle=agg.date_recolte
	order by agg.date_recolte);

	create table c6 as( 
		with agg as(
		select 
		code_insee, 
		numero_station 
		from bro_producteur_station 
		where numero_station is not null 
		group by 1, 2)
	select c5.*, 
	agg.numero_station 
	from c5 
	left join agg 
	on agg.code_insee=c5.code_insee order by c5.date_recolte);

	create table c7 as select 
	numero_station::int,
	date_recolte, 
	AVG(tmax_30j_sum)::float as tmax_30j_sum,
	AVG(tmax_10j_sum)::float as tmax_10j_sum,
	AVG(tmax_2j_sum)::float as tmax_2j_sum,
	AVG(tmax)::float as tmax,
	AVG(rr10)::float as rr10,
	AVG(rr_17j_sum)::float as rr_17j_sum,
	AVG(levee_brocolis_ideale)::float as levee_brocolis_ideale, 
	AVG(jour_froid_sum)::float as jour_froid_sum,
	SUM(quantite_initiale)::int as quantite_initiale,
	SUM(quantite_recolte)::int as quantite_recolte
	from c6 group by 1, 2 order by 2;
	
	return query select numero_station, date_recolte, tmax_30j_sum, tmax_10j_sum, tmax_2j_sum, tmax, rr10, rr_17j_sum, levee_brocolis_ideale, jour_froid_sum, quantite_initiale, quantite_recolte from c7 order by date_recolte;
	
	drop table if exists c1;
	drop table if exists c2;
	drop table if exists c3;
	drop table if exists c4;
	drop table if exists c5;
	drop table if exists c6;
	drop table if exists c7;
	
end;
$$ language plpgsql




create or replace function bro_verifications()
returns table(
nbre int, 
erreur VARCHAR(100)
)
as $$
begin
	
	return query
	with truc as(
	select count(prdcode), 'adhérents dans apports non listés dans table producteurs' as type from bro_apports where prdcode not in (select code_adherent from bro_producteur_station)
	union all
	select count(code_adherent), 'adhérents dans semis non listés dans table producteurs' as type from bro_semis where code_adherent not in (select code_adherent from bro_producteur_station)
	union all
	select count(prdcode), 'adhérents dans apports non listés dans semis' as type from bro_apports where prdcode not in (select code_adherent from bro_semis)
	union all
	(with agg as(select code_insee,date_reelle,  count(code_insee) as compte from meteo group by 1,2 order by 2 desc)
	select count(*), 'doublons météo' as type from agg where compte>1))
	select count::int, type::VARCHAR(100) from truc;
	
end; $$ language plpgsql;



create or replace function meteo_valeurs_manquantes()
--returns void
--as $$
--begin 
--	update meteo set etp=null where etp=-99;
--	update meteo set probawind70=null where probawind70=-99;
--	update meteo set probawind100=null where probawind100=-99;
--	update meteo set probafrost=null where probafrost=-99;
--	update meteo set probafog=null where probafog=-99;
--	update meteo set probarain=null where probarain=-99;
--	update meteo set tmax=null where tmax=-99;
--	update meteo set tmin=null where tmin=-99;
--	update meteo set rr10=null where rr10=-99;
--	update meteo set gust10m=null where gust10m=-99;
--	update meteo set wind10m=null where wind10m=-99;
--	update meteo set sunhours=null where sunhours=-99;
--end;
--$$ language plpgsql



create or replace function bro_predictions()
returns table(
blend int,
knn int,
xgb int,
keras int,
qte_reelle int,
sem int)
as $$
begin 
	
	drop table if exists c1;
	drop table if exists c2;
	drop table if exists c3;
	drop table if exists c4;
	drop table if exists c5;
	drop table if exists c6;
	
	create table c1 as(
	with agg as(
		select 
		bro_pred_vg + bro_pred_cleder as somme_par_jour, 
		extract(week from jour) as semaine, 
		extract(year from jour) as annee
		from predictions)
	select sum(agg.somme_par_jour) as qte_par_semaine,
	agg.semaine, agg.annee
	from agg 
	where semaine>=extract(week from current_date)-1 and annee=extract(year from current_date)
	group by 2, 3
	order by 2,3
	limit 3);

	create table c2 as(
	with agg as(
		select 
		pred_vg_fd_knn + pred_cleder_fd_knn as somme_par_jour, 
		extract(week from jour) as semaine, 
		extract(year from jour) as annee
		from predictions)
	select sum(agg.somme_par_jour) as qte_par_semaine,
	agg.semaine, agg.annee
	from agg 
	where semaine>=extract(week from current_date)-1 and annee=extract(year from current_date)
	group by 2, 3
	order by 2,3
	limit 3);

	create table c3 as(
	with agg as(
		select 
		pred_vg_fd_xgb + pred_cleder_fd_xgb as somme_par_jour, 
		extract(week from jour) as semaine, 
		extract(year from jour) as annee
		from predictions)
	select sum(agg.somme_par_jour) as qte_par_semaine,
	agg.semaine, agg.annee
	from agg 
	where semaine>=extract(week from current_date)-1 and annee=extract(year from current_date)
	group by 2, 3
	order by 2,3
	limit 3);

	create table c4 as(
	with agg as(
		select 
		pred_vg_fd_keras + pred_cleder_fd_keras as somme_par_jour, 
		extract(week from jour) as semaine, 
		extract(year from jour) as annee
		from predictions)
	select sum(agg.somme_par_jour) as qte_par_semaine,
	agg.semaine, agg.annee
	from agg 
	where semaine>=extract(week from current_date)-1 and annee=extract(year from current_date)
	group by 2, 3
	order by 2,3
	limit 3);	

	create table c5 as(
	with agg as(
		select 
		liv_qty as somme_par_jour, 
		extract(week from liv_date) as semaine, 
		extract(year from liv_date) as annee
		from bro_apports)
	select sum(agg.somme_par_jour) as qte_par_semaine,
	agg.semaine, agg.annee
	from agg 
	where semaine>=extract(week from current_date)-1 and annee=extract(year from current_date)
	group by 2, 3
	order by 2,3
	limit 3);

	create table c6 as select 
	c1.qte_par_semaine::int as out_blender,
	c2.qte_par_semaine::int as out_knn,
	c3.qte_par_semaine::int as out_xgb,
	c4.qte_par_semaine::int as out_keras,
	c5.qte_par_semaine::int as out_reel,
	c1.semaine::int 
	from c1 
	join c2 on c1.semaine=c2.semaine
	join c3 on c1.semaine=c3.semaine
	join c4 on c1.semaine=c4.semaine
	left join c5 on c1.semaine=c5.semaine
	order by semaine;

	return query select * from c6;
	
	drop table if exists c1;
	drop table if exists c2;
	drop table if exists c3;
	drop table if exists c4;
	drop table if exists c5;
	drop table if exists c6;

end; $$ language plpgsql;

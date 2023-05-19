CREATE OR REPLACE FUNCTION public.cf_flux5_week()
 RETURNS TABLE(id integer, sem integer, an integer, qte integer, t_min integer, t_max integer, pluie integer, ctmax integer, ctmin integer, cpluie integer)
 LANGUAGE plpgsql
AS $function$
begin
	
	drop table if exists cf_apports_complets_12345;
	drop table if exists cf_prov_12345;
	drop table if exists cf_prov_123456;
	drop table if exists cf_agg_day;

	create temp table cf_apports_complets_12345 as select * from cf_apports_sauv cas ;
	insert into cf_apports_complets_12345(date_recolte, code_adherent, quantite_recolte) select date_recolte, code_adherent, quantite_recolte from cf_apports;
	create table cf_prov_123456 as select sum(quantite_recolte) as quantite_recolte, date_recolte from cf_apports_complets_12345 group by 2;
	create table cf_prov_12345 as select b.quantite_recolte, a.jour as date_recolte from index_dates a left join cf_prov_123456 b on a.jour=b.date_recolte;	

	create temp table cf_agg_day as
	with agg_day as(
		with meteo_agg as(
			select
			date_reelle, 
			tmin, 
			tmax, 
			rr10,
			sum(tmax) over(partition by code_insee order by date_reelle rows between 38 preceding and current ROW) as cumultmax,
			sum(tmin) over(partition by code_insee order by date_reelle rows between 28 preceding and current ROW) as cumultmin,
			sum(rr10) over(partition by code_insee order by date_reelle rows between 28 preceding and current ROW) as cumulrr10
			from meteo where code_insee=29030
		)
		select
		ap.date_recolte,
		ap.quantite_recolte::int,
		m.tmin::int,
		m.tmax::int,
		m.rr10::int,
		m.cumultmax,
		m.cumultmin,
		m.cumulrr10
		from cf_prov_12345 ap 
		left join meteo_agg m 
		on ap.date_recolte=m.date_reelle
		where ap.date_recolte>='2016-02-08'
		order by ap.date_recolte asc)
	select 
	sum(quantite_recolte)::int as quantite_recolte,
	sum(tmin)::int as tmin,
	sum(tmax)::int as tmax,
	sum(rr10)::int as rr10,
	sum(cumultmax)::int as cumultmax,
	sum(cumultmin)::int as cumultmin,
	sum(cumulrr10)::int as cumulrr10,	
	extract(week from date_recolte)::int as semaine,
	extract(year from date_recolte)::int as annee
	from agg_day 
	group by 8, 9
	order by 9, 8;
		
	alter table cf_agg_day add idx serial;
	return query select idx, semaine, annee, quantite_recolte, tmin, tmax, rr10, cumultmax, cumultmin, cumulrr10 
		from cf_agg_day where tmin is not null order by annee, semaine;
	
	drop table if exists cf_apports_complets_12345;
	drop table if exists cf_prov_12345;
	drop table if exists cf_prov_123456;
	drop table if exists cf_agg_day;

end;
$function$
;

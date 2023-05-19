CREATE OR REPLACE FUNCTION public.cf_flux5_day()
 RETURNS TABLE(jour date, qte integer, t_min integer, t_max integer, ctmax integer, ctmin integer, cpluie integer)
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
	with meteo_agg as(
		select 
		date_reelle,
		tmin, 
		tmax,
		sum(tmax) over(partition by code_insee order by date_reelle rows between 38 preceding and current ROW) as cumultmax,
		sum(tmin) over(partition by code_insee order by date_reelle rows between 28 preceding and current ROW) as cumultmin,
		sum(rr10) over(partition by code_insee order by date_reelle rows between 28 preceding and current ROW) as cumulrr10
		from meteo 
		where code_insee=29030
	)
	select
	ap.date_recolte,
	ap.quantite_recolte::int,
	m.tmin::int,
	m.tmax::int,
	m.cumultmax::int,
	m.cumultmin::int,
	m.cumulrr10::int
	from cf_prov_12345 ap 
	left join meteo_agg m 
	on ap.date_recolte=m.date_reelle
	order by ap.date_recolte asc;
		
	
	return query select * from cf_agg_day where date_recolte>'2016-02-05' order by date_recolte;
	
	drop table if exists cf_apports_complets_12345;
	drop table if exists cf_prov_12345;
	drop table if exists cf_prov_123456;
	drop table if exists cf_agg_day;

end;
$function$
;

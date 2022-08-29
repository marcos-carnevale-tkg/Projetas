select
      a.razao_social
     ,aeo.name as nome_aeroporto_origem
     ,v.icao_aerodromo_origem as icao_aeroporto_origem
     ,concat(aeo.state, "/", aeo.country_iso) as estado_uf_origem
     ,aed.name as nome_aeroporto_destino
     ,v.icao_aerodromo_destino as icao_aeroporto_destino
     ,concat(aed.state, "/", aed.country_iso) as estado_uf_destino
     ,count(concat(v.icao_aerodromo_destino, ' - ', v.icao_aerodromo_destino)) as qtd_rota
from air_cia a
left join vra v 
    on a.icao = v.icao_empresa_aerea
left join aerodromos aeo
    on v.icao_aerodromo_origem = aeo.icao
left join aerodromos aed
    on v.icao_aerodromo_destino = aed.icao
group by 
   a.razao_social
  ,aeo.name
  ,icao_aerodromo_origem
  ,concat(aeo.state, "/", aeo.country_iso)
  ,aed.name
  ,icao_aerodromo_destino
  ,concat(aed.state, "/", aed.country_iso)
order by qtd_rota desc
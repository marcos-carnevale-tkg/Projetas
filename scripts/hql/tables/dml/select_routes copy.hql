with temp as
(
  select
       ae.name as nome_aeroporto
      ,ae.icao as icao_aeroporto
      ,aco.razao_social as nome_cia_aerea
      ,(
        case
            when vo.situacao_voo = 'REALIZADO' then 'Decolagens' else ''
        end) as Decolagens_Pousos
      ,(vo.icao_aerodromo_origem) as rotas
from aerodromos ae
left join vra vo
  on ae.icao = vo.icao_aerodromo_origem 
left join air_cia aco
  on vo.icao_empresa_aerea == aco.icao
where (
        case
            when vo.situacao_voo = 'REALIZADO' then 'Decolagens' else ''
        end)  != "" 
and aco.icao is not null
)
union all
(
  select
        ae.name as nome_aeroporto
        ,ae.icao as icao_aeroporto
        ,acd.razao_social as nome_cia_aerea
        ,(
          case
              when vd.situacao_voo = 'REALIZADO' then 'Pousos' else ''
          end) as Decolagens_Pousos
        ,(vd.icao_aerodromo_origem) as rotas
  from aerodromos ae
  left join vra vd
    on ae.icao = vd.icao_aerodromo_destino
  left join air_cia acd
    on vd.icao_empresa_aerea == acd.icao
  where (
          case
              when vd.situacao_voo = 'REALIZADO' then 'Pousos' else ''
          end)  != "" 
  and acd.icao is not null 
),
temp2 as
(
  select
      nome_aeroporto
      ,icao_aeroporto
      ,nome_cia_aerea
      ,Decolagens_Pousos
      ,rotas
  from temp

)
 
 


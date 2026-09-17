// API do Portal Sinequa (interno, atrás de login) — serve os blocos de dados do portal.
//
// Feijão com arroz:
//   · autenticação = 1 token compartilhado (header "x-portal-key" ou ?key=). Sem OAuth/sessão.
//   · dois backends com o MESMO SQL (ver dispatcher do mart() abaixo):
//       - Metabase export /json (db=3) — funciona de qualquer lugar via Tailscale, sem teto de linhas.
//       - Postgres direto (pg)         — mais robusto p/ produção; ativa com env de PG.
//   · cada rota devolve exatamente a forma que o front já consome (window.ANO, etc.),
//     então ligar no portal é trocar os const por fetch, sem mudar render.
//
// Rodar via Metabase/Tailscale (deste ambiente):
//   MB_URL=http://100.69.211.55:3000 PORTAL_TOKEN=sinequa2026 node portal.js
// Rodar via Postgres direto (no servidor, onde o pg é localhost):
//   npm install   # instala o pg (ver package.json)
//   PORTAL_DB=pg PGDATABASE=sinequa PORTAL_TOKEN=sinequa2026 node portal.js
//   (também aceita PGHOST/PGPORT/PGUSER/PGPASSWORD ou DATABASE_URL)
const http = require('http');
const path = require('path');
const fs   = require('fs');

// ---------- config ----------
function carregaEnv(arquivo){ try{ for(const l of fs.readFileSync(arquivo,'utf8').split('\n')){ const m=l.match(/^\s*([A-Z_]+)\s*=\s*(.*?)\s*$/); if(m && !l.trim().startsWith('#')) process.env[m[1]] ??= m[2].trim(); } }catch{} }
carregaEnv(path.join(__dirname,'..','sinequa-metabase','mb.env'));  // MB_API_KEY, MB_URL
carregaEnv(path.join(__dirname,'portal.env'));                      // opcional: PORTAL_TOKEN, MB_URL, PORTA

const PORTA  = Number(process.env.PORTAL_PORT || 8092);
const MB     = (process.env.MB_URL || 'http://localhost:3000').replace(/\/$/,'');
const MBKEY  = process.env.MB_API_KEY || '';
const TOKEN  = process.env.PORTAL_TOKEN || 'sinequa2026';
const ORIGENS = (process.env.PORTAL_ORIGENS || '*').split(',').map(s=>s.trim());

// ---------- consulta ao mart — dois backends, MESMO SQL ----------
// · Metabase export /json (default): funciona de qualquer lugar via Tailscale, sem teto de 2000 linhas.
// · Postgres direto (pg): mais robusto p/ produção; ativa quando há env de PG (rodando no servidor).
// Toggle: PORTAL_DB=pg força pg · PORTAL_DB=mb força Metabase · sem isso, usa pg se houver PGHOST/DATABASE_URL.
const _pgEnv  = process.env.DATABASE_URL || process.env.PGHOST || process.env.PGDATABASE;
const USA_PG  = (process.env.PORTAL_DB||'').toLowerCase()==='pg' || (!process.env.PORTAL_DB && !!_pgEnv);
const BACKEND = USA_PG ? 'pg' : 'metabase';

let _pool=null;
function pool(){
  if(_pool) return _pool;
  const { Pool, types } = require('pg');
  // Faz o pg devolver os tipos como o Metabase/JSON: numeric→float, int8→int, date→'YYYY-MM-DD',
  // timestamp→string (nunca objeto Date). Assim os blocos abaixo não mudam uma linha.
  types.setTypeParser(1700, v=> v==null?null:parseFloat(v));   // numeric
  types.setTypeParser(20,   v=> v==null?null:parseInt(v,10));  // int8 / bigint
  types.setTypeParser(1082, v=> v);                            // date        -> 'YYYY-MM-DD'
  types.setTypeParser(1114, v=> v);                            // timestamp   -> string
  types.setTypeParser(1184, v=> v);                            // timestamptz -> string
  _pool = process.env.DATABASE_URL
    ? new Pool({ connectionString: process.env.DATABASE_URL, max: 4 })
    : new Pool({ host: process.env.PGHOST||'localhost', port: +process.env.PGPORT||5432,
                 database: process.env.PGDATABASE||'sinequa',
                 user: process.env.PGUSER||process.env.USER||require('os').userInfo().username,
                 password: process.env.PGPASSWORD, max: 4 });
  // um cliente ocioso que cai não pode derrubar o processo — só loga e o pool se recupera na próxima query
  _pool.on('error', e=>console.error('[pg pool]', e.message));
  return _pool;
}
async function martPg(sql){ const r = await pool().query(sql); return r.rows; }
async function martMb(sql){
  const body = new URLSearchParams({ query: JSON.stringify({type:'native',database:3,native:{query:sql}}) }).toString();
  const r = await fetch(MB+'/api/dataset/json',{ method:'POST',
    headers:{'x-api-key':MBKEY,'Content-Type':'application/x-www-form-urlencoded'},
    body, signal: AbortSignal.timeout(30000) });
  const t = await r.text();
  let j; try{ j = JSON.parse(t); }catch{ throw new Error('mart '+r.status+': '+t.slice(0,150)); }
  if(!Array.isArray(j)) throw new Error(j && j.error ? (''+j.error).slice(0,200) : 'resposta inesperada do mart');
  return j;
}
const mart = USA_PG ? martPg : martMb;
const N = v => v==null?0:Number(v);

// ---------- blocos ----------
// ANO — window.ANO = { meses:[{mes,v25,v26,o25,o26,b25,b26,c25,c26,d25,d26,no25,no26,nv25,nv26}], ativos:[{ativo,venda}], presc:{top,outros,total} }
async function blocoAno(){
  const [mens, ativos, presc] = await Promise.all([
    mart(`SELECT ano,mes,round(venda) v,round(orcado) o,round(venda_bruta) b,round(coalesce(custo,0)) c,round(desconto) d,n_orcamentos no,n_vendas nv FROM mart.mensal WHERE ano IN (2025,2026) ORDER BY ano,mes`),
    mart(`SELECT ativo_principal ativo, round(sum(venda)) venda FROM mart.venda_orcado_detalhe WHERE ano=2026 AND venda>0 GROUP BY 1 ORDER BY venda DESC LIMIT 12`),
    mart(`SELECT medico, round(sum(venda)) venda FROM mart.medico_diario WHERE ano=2026 GROUP BY 1 ORDER BY venda DESC`),
  ]);
  const meses = [];
  for(let m=1;m<=12;m++){
    const a = mens.find(r=>r.ano===2025 && r.mes===m) || {};
    const b = mens.find(r=>r.ano===2026 && r.mes===m) || {};
    meses.push({ mes:m,
      v25:N(a.v), v26:N(b.v), o25:N(a.o), o26:N(b.o), b25:N(a.b), b26:N(b.b),
      c25:N(a.c), c26:N(b.c), d25:N(a.d), d26:N(b.d),
      no25:N(a.no), no26:N(b.no), nv25:N(a.nv), nv26:N(b.nv) });
  }
  const top = presc.slice(0,10).map(r=>({medico:r.medico, venda:N(r.venda)}));
  const total = presc.reduce((s,r)=>s+N(r.venda),0);
  const outros = total - top.reduce((s,r)=>s+r.venda,0);
  return { meses, ativos: ativos.map(r=>({ativo:r.ativo, venda:N(r.venda)})), presc:{ top, outros, total } };
}

// MÊS — { TOT, DIACAL, ACUM, DIA, REM, SUPERMETA, PRESCR } — mês corrente, dinâmico
async function blocoMes(){
  const [dias, meds, det, dscRows] = await Promise.all([
    mart(`SELECT to_char(data,'DD') dd, (array['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'])[extract(dow from data)::int+1] dow,
            round(venda) venda, round(orcado) orcado, round(meta) meta, round(venda_bruta) vb,
            round(venda_acum) va, round(meta_acum) ma, (data<=CURRENT_DATE) ispast
          FROM mart.diario WHERE date_trunc('month',data)=date_trunc('month',CURRENT_DATE) ORDER BY data`),
    mart(`SELECT medico m, round(sum(orcado)) orc, round(sum(venda)) vda,
            round((sum(desconto)/nullif(sum(venda_bruta),0))::numeric,4) descp
          FROM mart.medico_diario WHERE date_trunc('month',data)=date_trunc('month',CURRENT_DATE) GROUP BY 1 ORDER BY vda DESC`),
    mart(`SELECT medico, paciente, nr_orcamento nr, ativo_principal ativo, round(orcado) o, round(venda) v
          FROM mart.venda_orcado_detalhe WHERE ano=extract(year from current_date)::int AND mes=extract(month from current_date)::int`),
    mart(`SELECT nrorc, round(sum(prcobr)) brt, round(sum(vrdsc)) dsc FROM mart.f_venda
          WHERE dtentr>=date_trunc('month',current_date) AND dtentr<=current_date AND nrorc>0 GROUP BY nrorc`),
  ]);
  const DIACAL = dias.map(d=>({dd:d.dd, dow:d.dow, venda:N(d.venda), orcado:N(d.orcado), meta:N(d.meta)}));
  const ACUM   = dias.map(d=>({dd:d.dd, va: d.ispast? N(d.va): null, ma: N(d.ma)}));
  const DIA    = DIACAL.filter(d=>d.orcado>0);
  const past = dias.filter(d=>d.ispast), fut = dias.filter(d=>!d.ispast);
  const v_liq = past.reduce((s,d)=>s+N(d.venda),0), v_bruta = past.reduce((s,d)=>s+N(d.vb),0);
  const orc = past.reduce((s,d)=>s+N(d.orcado),0);
  const meta_ate = past.reduce((s,d)=>s+N(d.meta),0), meta_mes = dias.reduce((s,d)=>s+N(d.meta),0);
  const ultDia = [...past].reverse().find(d=>N(d.venda)>0);
  const ref = ultDia? new Date().getFullYear()+'-'+String(new Date().getMonth()+1).padStart(2,'0')+'-'+ultDia.dd : null;
  const TOT = { v_liq, v_bruta, orc, meta_ate, meta_mes, ref };
  const SUPERMETA = Math.round(meta_mes*1.065);
  const REM = { falta: Math.max(meta_mes - v_liq,0), dias: fut.length, dias_uteis: fut.filter(d=>N(d.meta)>0).length };
  const medicos = meds.map(r=>({m:r.m, orc:N(r.orc), vda:N(r.vda), desc:N(r.descp)}));
  const idx={}; medicos.forEach((x,i)=>idx[x.m]=i);
  const detArr = det.filter(r=>idx[r.medico]!=null).map(r=>[idx[r.medico], r.paciente, N(r.nr), r.ativo, N(r.o), N(r.v)]);
  const dsc={}; dscRows.forEach(r=>{ dsc[N(r.nrorc)]=[N(r.brt), N(r.dsc)]; });
  const PRESCR = { ref, medicos, det: detArr, dsc };
  return { TOT, DIACAL, ACUM, DIA, REM, SUPERMETA, PRESCR };
}

// DIA — window.HOJE = { hoje, dias:[{d,lbl}], byDay:{ 'YYYY-MM-DD': {ag:{venda,orcado,meta,bruta,dow}, nped, medicos:[{m,vda,brt,dsc}], det:[[mi,pac,nr,ativo,orc,vda]], dsc:{nr:[brt,dsc]}} } }
function _iso(d){ const x=new Date(d); return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0'); }
async function blocoDia(){
  const [days, forc, vod, diario] = await Promise.all([
    mart(`SELECT dtentr::date d, count(distinct nrorc) nped FROM mart.f_venda WHERE dtentr>=current_date-25 AND dtentr<=current_date AND nrorc>0 GROUP BY 1 ORDER BY 1 DESC LIMIT 11`),
    mart(`SELECT dtentr::date d, nrorc, round(sum(prcobr)) brt, round(sum(vrdsc)) dsc FROM mart.f_venda WHERE dtentr>=current_date-25 AND dtentr<=current_date AND nrorc>0 GROUP BY 1,2`),
    mart(`SELECT medico, paciente, nr_orcamento nr, ativo_principal ativo, round(orcado) o, round(venda) v FROM mart.venda_orcado_detalhe
          WHERE nr_orcamento IN (SELECT DISTINCT nrorc FROM mart.f_venda WHERE dtentr>=current_date-25 AND dtentr<=current_date AND nrorc>0)`),
    mart(`SELECT data::date d, (array['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'])[extract(dow from data)::int+1] dow, round(venda) venda, round(orcado) orcado, round(meta) meta, round(venda_bruta) bruta FROM mart.diario WHERE data>=current_date-25 AND data<=current_date`),
  ]);
  const dayList = days.map(r=>_iso(r.d));
  const npedBy = {}; days.forEach(r=>npedBy[_iso(r.d)]=N(r.nped));
  const diarioBy = {}; diario.forEach(r=>diarioBy[_iso(r.d)]={venda:N(r.venda),orcado:N(r.orcado),meta:N(r.meta),bruta:N(r.bruta),dow:r.dow});
  // f_venda: nrorc -> {day, brt, dsc}
  const forcByNr = {}; forc.forEach(r=>{ forcByNr[N(r.nrorc)]={day:_iso(r.d), brt:N(r.brt), dsc:N(r.dsc)}; });
  // vod agrupado por nrorc
  const vodByNr = {}; vod.forEach(r=>{ (vodByNr[N(r.nr)] ||= []).push({medico:r.medico, paciente:r.paciente, ativo:r.ativo, o:N(r.o), v:N(r.v)}); });
  const byDay = {};
  for(const day of dayList){
    const nrs = forc.filter(r=>_iso(r.d)===day).map(r=>N(r.nrorc));
    const medMap = {}, dsc = {}, orders = [];
    for(const nr of nrs){
      const f = forcByNr[nr] || {brt:0,dsc:0}; dsc[nr]=[f.brt, f.dsc];
      const rows = vodByNr[nr] || [];
      const med = rows[0]?.medico || '—', pac = rows[0]?.paciente || '—';
      const m = (medMap[med] ||= {m:med, vda:0, brt:0, dsc:0});
      m.brt += f.brt; m.dsc += f.dsc;
      rows.forEach(x=>{ m.vda += x.v; });
      orders.push({nr, pac, rows});
    }
    const medicos = Object.values(medMap).sort((a,b)=>b.vda-a.vda);
    const idx = {}; medicos.forEach((x,i)=>idx[x.m]=i);
    const det = [];
    orders.forEach(o=>{ o.rows.forEach(x=>{ const mi=idx[x.medico]; if(mi!=null) det.push([mi, o.pac, o.nr, x.ativo, x.o, x.v]); }); });
    byDay[day] = { ag: diarioBy[day] || {venda:0,orcado:0,meta:0,bruta:0,dow:''}, nped: npedBy[day]||nrs.length, medicos, det, dsc };
  }
  const dias = dayList.map(d=>({d, lbl: d.slice(8)+'/'+d.slice(5,7)}));
  return { hoje: dayList[0], dias, byDay };
}

// PRODUÇÃO — window.PROD = { hoje, pcpHoje, ref, semanas:[{wk,lbl,prazo,atraso,ematraso,producao,dias:[{dow,dd,prazo,atraso,ematraso,producao}]}], atrByWk:{wk:[...]}, pcpDias:[{d,lbl}], pcpByDia:{dia:[{ped,paciente,envio,pend,stMin,formulas:[{serier,ativo,etapa,st}]}]} }
const _DOW=['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
function _monday(d){ const x=new Date(d+'T00:00:00'); const g=x.getDay(); x.setDate(x.getDate()-((g+6)%7)); return _isoD(x); }
function _isoD(x){ return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0'); }
function _ddmm(d){ return d.slice(8)+'/'+d.slice(5,7); }
const _STmin={'Em atraso':0,'Em produção':1,'Pronta com Atraso':2,'Pronta no Prazo':3};
async function blocoProd(){
  const [sem, atr, fila] = await Promise.all([
    mart(`SELECT data_prevista_entrega::date d, status_entrega st, count(*) n FROM mart.pcp
          WHERE data_prevista_entrega >= current_date - interval '8 weeks' AND data_prevista_entrega < current_date + interval '2 weeks' GROUP BY 1,2`),
    mart(`SELECT data_prevista_entrega::date prev, nrrqu ped, chave_formula chave, paciente, to_char(data_hora_saida_lab,'YYYY-MM-DD HH24:MI') saida, tipo_envio envio, status_entrega st
          FROM mart.pcp WHERE status_entrega IN ('Pronta com Atraso','Em atraso') AND data_prevista_entrega >= current_date - interval '8 weeks'`),
    mart(`SELECT data_prevista_entrega::date d, nrrqu ped, serier, paciente, tipo_envio envio, situacao etapa, status_entrega st
          FROM mart.pcp WHERE data_prevista_entrega >= current_date - interval '12 days' AND data_prevista_entrega < current_date + interval '6 days' ORDER BY data_prevista_entrega, nrrqu, serier`),
  ]);
  // ativos por pedido (nrrqu -> [ativo...]) só dos pedidos da fila
  const pedSet=[...new Set(fila.map(r=>N(r.ped)))];
  let ativoByPed={};
  if(pedSet.length){
    const av=await mart(`SELECT f.nrrqu ped, v.ativo_principal ativo FROM mart.f_venda f JOIN mart.venda_orcado_detalhe v ON v.nr_orcamento=f.nrorc
                         WHERE f.nrrqu IN (${pedSet.join(',')})`);
    av.forEach(r=>{ (ativoByPed[N(r.ped)] ||= []).push(r.ativo); });
  }
  // ---- semanas ----
  const dayAgg={}; // d -> {prazo,atraso,ematraso,producao}
  sem.forEach(r=>{ const d=_isoD(new Date(r.d)); const o=(dayAgg[d] ||= {prazo:0,atraso:0,ematraso:0,producao:0});
    if(r.st==='Pronta no Prazo')o.prazo+=N(r.n); else if(r.st==='Pronta com Atraso')o.atraso+=N(r.n); else if(r.st==='Em atraso')o.ematraso+=N(r.n); else if(r.st==='Em produção')o.producao+=N(r.n); });
  const wkMap={};
  Object.keys(dayAgg).forEach(d=>{ const wk=_monday(d); (wkMap[wk] ||= {}); wkMap[wk][d]=dayAgg[d]; });
  const semanas = Object.keys(wkMap).sort().map(wk=>{
    const dias=[]; let prazo=0,atraso=0,ematraso=0,producao=0;
    for(let i=0;i<7;i++){ const dt=new Date(wk+'T00:00:00'); dt.setDate(dt.getDate()+i); const d=_isoD(dt); const a=wkMap[wk][d]||{prazo:0,atraso:0,ematraso:0,producao:0};
      prazo+=a.prazo;atraso+=a.atraso;ematraso+=a.ematraso;producao+=a.producao;
      dias.push({dow:_DOW[dt.getDay()], dd:String(dt.getDate()).padStart(2,'0'), prazo:a.prazo,atraso:a.atraso,ematraso:a.ematraso,producao:a.producao}); }
    const end=new Date(wk+'T00:00:00'); end.setDate(end.getDate()+6);
    return { wk, lbl:_ddmm(wk)+'–'+_ddmm(_isoD(end)), prazo,atraso,ematraso,producao, dias };
  });
  const hoje = _monday(_isoD(new Date()));
  // ---- atrByWk ----
  const atrByWk={}; atr.forEach(r=>{ const wk=_monday(_isoD(new Date(r.prev))); (atrByWk[wk] ||= []).push({ped:N(r.ped),chave:r.chave,paciente:r.paciente,prev:_isoD(new Date(r.prev)),saida:r.saida,envio:r.envio,st:r.st}); });
  // ---- pcpByDia ----
  const pcpByDia={}, daysSeen=new Set();
  const byDayPed={}; // d -> ped -> {paciente,envio,formulas:[]}
  fila.forEach(r=>{ const d=_isoD(new Date(r.d)); daysSeen.add(d); const ped=N(r.ped);
    const dm=(byDayPed[d] ||= {}); const p=(dm[ped] ||= {ped,paciente:r.paciente,envio:r.envio,formulas:[]});
    p.formulas.push({serier:N(r.serier), etapa:r.etapa, st:r.st}); });
  Object.keys(byDayPed).forEach(d=>{
    pcpByDia[d]=Object.values(byDayPed[d]).map(p=>{
      const av=ativoByPed[p.ped]||[];
      p.formulas.forEach((f,i)=>{ f.ativo = av[i] || av[0] || '—'; });
      const pend=p.formulas.filter(f=>f.st==='Em produção'||f.st==='Em atraso').length;
      const stMin=Math.min(...p.formulas.map(f=>_STmin[f.st]!=null?_STmin[f.st]:3));
      return {ped:p.ped, paciente:p.paciente, envio:p.envio, pend, stMin, formulas:p.formulas};
    }).sort((a,b)=>a.stMin-b.stMin);
  });
  const pcpDiasAll=[...daysSeen].filter(d=>d<=_isoD(new Date())).sort();
  const pcpDias = pcpDiasAll.map(d=>({d, lbl:_ddmm(d)+' '+_DOW[new Date(d+'T00:00:00').getDay()]}));
  const pcpHoje = pcpDias.length? pcpDias[pcpDias.length-1].d : _isoD(new Date());
  return { hoje, pcpHoje, ref: _isoD(new Date()), semanas, atrByWk, pcpDias, pcpByDia };
}

// BUSCA — window.BUSCA = { ref, pacientes:[{nome,nped,orcado,vendido,ultima,medicos:[],cadastro:{cpf,rg:{num,orgao,uf},email,nascimento,cliente_desde,endereco:{...}},orcs:[{nr,data,dorc,medico,orcado,vendido,ativos:[{ativo,orcado,vendido}],desc}]}] }
// cadastro sai do stg.dim_cliente (join por CDCLI, via pedido: f_venda/f_orcamento.nr→cdcli).
// Telefone NÃO vem (o Fórmula não guarda — fica no CRM). No portal ao vivo (atrás de login) vem completo.
function _cad(r){
  if(!r) return null;
  const cep = r.nrcep ? String(r.nrcep).replace(/\D/g,'').replace(/^(\d{5})(\d{3})$/,'$1-$2') : null;
  const temEnd = r.ender||r.bairr||r.munic;
  return {
    cpf: r.nrcnpj||null,
    rg: r.nrinscr ? { num:r.nrinscr, orgao:r.oerg||null, uf:r.ufrg||null } : null,
    email: r.email||null,
    nascimento: r.dtnas||null,
    cliente_desde: r.dtcad||null,
    endereco: temEnd ? { logradouro:r.ender||null, numero:r.endnr||null, complemento:r.endcp||null,
                         bairro:r.bairr||null, cep, cidade:r.munic||null, uf:r.unfed||null } : null
  };
}
async function blocoBusca(){
  const [vod, fv, fo] = await Promise.all([
    mart(`SELECT medico, paciente, nr_orcamento nr, ativo_principal ativo, round(orcado) o, round(venda) v FROM mart.venda_orcado_detalhe WHERE ano=extract(year from current_date)::int`),
    mart(`SELECT nrorc nr, max(cdcli) cdcli, to_char(max(dtentr),'YYYY-MM-DD') data, round(sum(prcobr)) brt, round(sum(vrdsc)) dscv FROM mart.f_venda WHERE nrorc>0 AND dtentr>=date_trunc('year',current_date) GROUP BY nrorc`),
    mart(`SELECT nrorc nr, max(cdcli) cdcli, to_char(max(dtentr),'YYYY-MM-DD') dorc FROM mart.f_orcamento WHERE nrorc>0 AND dtentr>=date_trunc('year',current_date) GROUP BY nrorc`),
  ]);
  // cadastro é opcional: se o extract ainda não trouxe as colunas novas (RG/endereço), degrada sem quebrar o busca.
  let cad=[];
  try{ cad = await mart(`SELECT cdcli, nrcnpj, nrinscr, oerg, ufrg, email, to_char(dtnas,'YYYY-MM-DD') dtnas, to_char(dtcad,'YYYY-MM-DD') dtcad, ender, endnr, endcp, bairr, nrcep, munic, unfed FROM stg.dim_cliente`); }
  catch(e){ console.warn('[busca] cadastro indisponível — '+e.message.slice(0,80)); }
  const fvMap={}; fv.forEach(r=>{ fvMap[N(r.nr)]={data:r.data, brt:N(r.brt), dscv:N(r.dscv), cdcli:N(r.cdcli)}; });
  const foMap={}; fo.forEach(r=>{ foMap[N(r.nr)]={dorc:r.dorc, cdcli:N(r.cdcli)}; });
  const cadMap={}; cad.forEach(r=>{ cadMap[N(r.cdcli)]=r; });
  const pacMap={};
  vod.forEach(r=>{
    const nome=r.paciente||'—'; const p=(pacMap[nome] ||= {nome, orders:{}, medicos:new Set(), cdcli:null});
    const nr=N(r.nr); const ord=(p.orders[nr] ||= {nr, medico:r.medico, orcado:0, vendido:0, ativos:[]});
    ord.orcado+=N(r.o); ord.vendido+=N(r.v); ord.ativos.push({ativo:r.ativo, orcado:N(r.o), vendido:N(r.v)});
    if(r.medico) p.medicos.add(r.medico);
    if(!p.cdcli){ const c=(fvMap[nr]||{}).cdcli || (foMap[nr]||{}).cdcli; if(c) p.cdcli=c; }
  });
  const pacientes = Object.values(pacMap).map(p=>{
    const orcs = Object.values(p.orders).map(o=>{
      const f=fvMap[o.nr]||{};
      return { nr:o.nr, data:f.data||null, dorc:(foMap[o.nr]||{}).dorc||f.data||null, medico:o.medico, orcado:o.orcado, vendido:o.vendido,
               ativos:o.ativos, desc: (f.brt? Math.round(f.dscv/f.brt*1e4)/1e4 : null) };
    }).sort((a,b)=> (b.data||b.dorc||'').localeCompare(a.data||a.dorc||''));
    const vendido=orcs.reduce((s,o)=>s+o.vendido,0), orcado=orcs.reduce((s,o)=>s+o.orcado,0);
    const ultima=orcs.map(o=>o.data).filter(Boolean).sort().at(-1)||null;
    return { nome:p.nome, nped:orcs.length, orcado, vendido, ultima, medicos:[...p.medicos], cadastro:_cad(cadMap[p.cdcli]), orcs };
  }).sort((a,b)=>b.vendido-a.vendido);
  return { ref: new Date().toISOString().slice(0,10), pacientes };
}

const BLOCOS = { ano: blocoAno, mes: blocoMes, dia: blocoDia, producao: blocoProd, busca: blocoBusca };

// ---------- HTTP ----------
function responde(res, origem, status, corpo){
  const h = {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'};
  if(ORIGENS.includes('*')){ h['Access-Control-Allow-Origin']='*'; }
  else if(origem && ORIGENS.includes(origem)){ h['Access-Control-Allow-Origin']=origem; h['Vary']='Origin'; }
  res.writeHead(status,h); res.end(JSON.stringify(corpo));
}
const servidor = http.createServer(async (req,res)=>{
  const origem = req.headers.origin;
  if(req.method==='OPTIONS'){ res.writeHead(204,{'Access-Control-Allow-Origin':ORIGENS.includes('*')?'*':(ORIGENS.includes(origem)?origem:ORIGENS[0]||'*'),'Access-Control-Allow-Methods':'GET, OPTIONS','Access-Control-Allow-Headers':'x-portal-key','Access-Control-Max-Age':'86400'}); return res.end(); }
  const url = new URL(req.url,'http://x');
  if(url.pathname==='/portal/saude') return responde(res,origem,200,{ok:true,hora:new Date().toISOString(),backend:BACKEND,fonte:USA_PG?(process.env.DATABASE_URL?'DATABASE_URL':(process.env.PGHOST||'localhost')+':'+(process.env.PGPORT||5432)+'/'+(process.env.PGDATABASE||'sinequa')):MB});
  const m = url.pathname.match(/^\/portal\/([a-z]+)$/);
  if(!m || !BLOCOS[m[1]]) return responde(res,origem,404,{erro:'rota desconhecida'});
  // auth — token compartilhado
  const key = req.headers['x-portal-key'] || url.searchParams.get('key') || '';
  if(key!==TOKEN) return responde(res,origem,401,{erro:'não autorizado'});
  try{
    const dados = await BLOCOS[m[1]]();
    return responde(res,origem,200,dados);
  }catch(e){
    console.error('[erro]',m[1],e.message);
    const ocupado = /timeout|ETIMEDOUT|canceling statement/i.test(e.message||'');
    return responde(res,origem,ocupado?503:500,{erro: ocupado?'Atualizando os dados, tente em segundos.':'Não foi possível consultar agora.'});
  }
});
servidor.listen(PORTA,()=>{
  const fonte = USA_PG ? (process.env.DATABASE_URL?'DATABASE_URL':`${process.env.PGHOST||'localhost'}:${process.env.PGPORT||5432}/${process.env.PGDATABASE||'sinequa'}`) : MB;
  console.log(`API do Portal em http://localhost:${PORTA}  (mart via ${BACKEND} → ${fonte})`);
  console.log(`  teste: curl -s "http://localhost:${PORTA}/portal/ano?key=${TOKEN}"`);
});

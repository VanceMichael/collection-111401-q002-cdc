export const initialState = { generation: 1, mode: 'normal', clock: 1000, confirmed: 40 };
export async function stop(state) {  }
export async function routes(context) {
 const { request, url, body, payload, state, json, text, binary } = context;

 const fields = state.generation === 1 ? ['sku','quantity'] : ['sku','quantity','warehouse'];
 if (url.pathname === '/snapshot') return state.mode==='expired' ? json({snapshot:'stock-rebuilt',barrier:50,relation:{id:7,epoch:2,fields:['sku','quantity','warehouse']},rows:[['A',12,null],['B',20,null],['C',5,'east']],stream:'/changes?after=50'}) : json({ snapshot:'stock-base', barrier:40, relation:{id:7,epoch:1,fields:['sku','quantity']}, rows:[['A',10],['B',20]], stream:'/changes?after=40' });
 if (url.pathname === '/changes') {
   const after = Number(url.searchParams.get('after') ?? 40);
   const all = [
    {lsn:41,type:'begin',tx:'t1'}, {lsn:42,type:'row',tx:'t1',relation:7,epoch:1,values:['A',12]},
    {lsn:43,type:'begin',tx:'t2'}, {lsn:44,type:'row',tx:'t2',relation:7,epoch:1,values:['B',0]},
    {lsn:45,type:'abort',tx:'t2'}, {lsn:46,type:'commit',tx:'t1'},
    {lsn:47,type:'relation',relation:7,epoch:2,fields:['sku','quantity','warehouse']},
    {lsn:48,type:'begin',tx:'t3'}, {lsn:49,type:'row',tx:'t3',relation:7,epoch:2,values:['C',5,'east']}, {lsn:50,type:'commit',tx:'t3'}];
   const events = all.filter(event=>event.lsn>after).slice(0,3);
   if (state.mode==='expired' && after<46) return json({error:'变更保留窗口已过', rebuild:'/snapshot'},410);
   return json({events,next:events.at(-1)?.lsn??after,high:50});
 }
 if (url.pathname === '/confirm' && request.method === 'POST') {
   if (![40,46,50].includes(payload.lsn) || payload.lsn < state.confirmed) return json({error:'不是可确认的事务位置'},409);
   state.confirmed=payload.lsn; return json({confirmed:state.confirmed});
 }
 if (url.pathname === '/status') return json({confirmed:state.confirmed,high:50});

}

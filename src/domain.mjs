import { capture } from './client.mjs';
export async function receiveCurrent(base, directory) {
 const { bytes, receipt } = await capture(base + '/snapshot', directory);
 const data = JSON.parse(bytes);
 return { receipt: receipt.id, snapshot: data.snapshot, barrier: data.barrier, stocks: data.rows.map(values => Object.fromEntries(data.relation.fields.map((field,index)=>[field,values[index]]))) };
}

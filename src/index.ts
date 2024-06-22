import { getStationData } from "./02-foo";
console.time("getStationData");
const result = await getStationData();
console.log(result);
console.timeEnd("getStationData");

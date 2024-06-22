/*
Naive implementation, single threaded
local runtime: ~11m30s
*/
import { open } from "node:fs/promises";

const fd = await open("data/measurements-smol.txt");
const stream = fd.createReadStream();

const stations = new Map<
	string,
	{
		min: number;
		max: number;
		mean: number;
		sum: number;
		measurements: number;
	}
>();

let leftover = Buffer.alloc(0);

const size = (await fd.stat()).size;
let bytesRead = 0;

export async function getStationData() {
	await readFile();
	updateMean();
	return stations;
}

async function readFile() {
	for await (const c of stream) {
		const chunk = c as Buffer;
		// a chunk could end in the middle of a line (or multi-byte character for that matter)
		// therefore we need to buffer the remaining bytes after the last \n of the previous chunk
		// and prepend it to the current chunk

		// find the last newline character in the chunk
		const lastNewline = chunk.lastIndexOf("\n");

		// slice the chunk from the beginning to the last newline character
		// and prepend the leftover bytes from the previous chunk
		const chunkContent = Buffer.from([
			...leftover,
			...chunk.subarray(0, lastNewline),
		]);

		// set the leftover bytes to the remaining bytes after the last newline character
		leftover = chunk.subarray(lastNewline + 1);

		processChunk(chunkContent);

		bytesRead += chunk.length;
		process.stdout.write(`${((bytesRead / size) * 100).toFixed(2)}%\r`);
	}
}

const processChunk = (chunk: Buffer) => {
	const chunkContent = chunk.toString();
	const lines = chunkContent.split("\n");

	for (const line of lines) {
		const [station, measurement] = line.split(";");
		if (!station || !measurement) {
			throw new Error("Invalid line");
		}

		const value = Number.parseFloat(measurement);

		if (!stations.has(station)) {
			stations.set(station, {
				min: value,
				max: value,
				mean: value,
				sum: value,
				measurements: 1,
			});
		} else {
			const stationData = stations.get(station);
			if (!stationData) {
				throw new Error("Station data not found");
			}
			stationData.min = Math.min(stationData.min, value);
			stationData.max = Math.max(stationData.max, value);
			stationData.sum += value;
			stationData.measurements++;
			stationData.mean = Number.POSITIVE_INFINITY;
		}
	}
};

function updateMean() {
	for (const [, data] of stations) {
		if (data.mean === Number.POSITIVE_INFINITY) {
			data.mean = data.sum / data.measurements;
		}
	}
}

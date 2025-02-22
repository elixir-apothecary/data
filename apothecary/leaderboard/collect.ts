import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { setTimeout } from "timers/promises";
import * as parquet from 'parquetjs';

interface Rank {
    date: string;
    name: string;
    chest?: boolean;
    total: number;
    total_points_earned_by_tvl: number;
    total_tvl_usd: number;
    total_direct_referrals?: number;
    total_indirect_referrals?: number;
    total_points_earned_by_referrals?: number;
    rank: number;
}

interface LeaderboardResponse {
    ranks: Rank[];
    totalCount: number;
}

async function fetchLeaderboard(first: number = 25, offset: number = 0): Promise<LeaderboardResponse> {
    const response = await fetch(
        `https://api.points.elixir.xyz/api/scores?first=${first}&offset=${offset}`
    );
    return await response.json();
}

async function fetchAllRanks(): Promise<Rank[]> {
    const perPage = 5000;
    const interval = 500;
    const firstPage = await fetchLeaderboard(perPage, 0);
    const totalPages = Math.ceil(firstPage.totalCount / perPage);
    console.log(`Fetching ${totalPages} pages...`);

    const allRanks: Rank[] = [...firstPage.ranks];

    for (let page = 1; page < totalPages; page++) {
        await setTimeout(interval)
        const offset = page * perPage;
        const response = await fetchLeaderboard(perPage, offset);
        allRanks.push(...response.ranks);

        if (page % 10 === 0) {
            console.log(`Fetched ${page}/${totalPages} pages`);
        }
    }

    return allRanks;
}

async function main() {
    try {
        const ranks = await fetchAllRanks();
        console.log(`Retrieved ${ranks.length} total ranks`);

        const currentDir = dirname(fileURLToPath(import.meta.url));
        const outputPath = join(currentDir, 'leaderboard.parquet');

        const schema = new parquet.ParquetSchema({
            date: { type: 'UTF8', compression: 'SNAPPY' },
            name: { type: 'UTF8', compression: 'SNAPPY' },
            chest: { type: 'BOOLEAN', optional: true, compression: 'SNAPPY' },
            total: { type: 'DOUBLE', optional: true, compression: 'SNAPPY' },
            total_points_earned_by_tvl: { type: 'DOUBLE', optional: true, compression: 'SNAPPY' },
            total_tvl_usd: { type: 'DOUBLE', optional: true, compression: 'SNAPPY' },
            total_direct_referrals: { type: 'INT64', optional: true, compression: 'SNAPPY' },
            total_indirect_referrals: { type: 'INT64', optional: true, compression: 'SNAPPY' },
            total_points_earned_by_referrals: { type: 'DOUBLE', optional: true, compression: 'SNAPPY' },
            rank: { type: 'INT64', optional: true, compression: 'SNAPPY' },
        });

        const writer = await parquet.ParquetWriter.openFile(schema, outputPath);

        // 各行のデータを正しい形式に変換して書き込み
        for (const rank of ranks) {
            await writer.appendRow({
                date: rank.date || '',
                name: rank.name || '',
                chest: rank.chest ?? null,
                total: rank.total ?? null,
                total_points_earned_by_tvl: rank.total_points_earned_by_tvl ?? null,
                total_tvl_usd: rank.total_tvl_usd ?? null,
                total_direct_referrals: rank.total_direct_referrals ?? null,
                total_indirect_referrals: rank.total_indirect_referrals ?? null,
                total_points_earned_by_referrals: rank.total_points_earned_by_referrals ?? null,
                rank: rank.rank ?? null
            });
        }

        await writer.close();
        console.log(`Saved leaderboard data to ${outputPath}`);
    } catch (error) {
        console.error('Error:', error);
    }
}

main();

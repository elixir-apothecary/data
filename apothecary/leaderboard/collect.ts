import { writeFile } from 'fs/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { setTimeout } from "timers/promises";

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
        const outputPath = join(currentDir, 'leaderboard.json');

        await writeFile(outputPath, JSON.stringify(ranks, null, 2));
        console.log(`Saved leaderboard data to ${outputPath}`);
    } catch (error) {
        console.error('Error:', error);
    }
}

main();

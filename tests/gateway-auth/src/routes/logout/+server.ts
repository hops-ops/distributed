import { signOut } from '../../../.generated/auth';
export async function POST(event) { return signOut(event); }

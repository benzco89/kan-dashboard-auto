import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";
import crypto from "crypto";

const COOKIE_NAME = "kan_dashboard_session";
const COOKIE_MAX_AGE = 60 * 60 * 24 * 7; // 7 days

function createSessionToken(secret: string): string {
  const timestamp = Date.now().toString();
  const signature = crypto
    .createHmac("sha256", secret)
    .update(timestamp)
    .digest("hex");
  return `${timestamp}.${signature}`;
}

export function verifySessionToken(token: string, secret: string): boolean {
  const [timestamp, signature] = token.split(".");
  if (!timestamp || !signature) return false;

  const expectedSignature = crypto
    .createHmac("sha256", secret)
    .update(timestamp)
    .digest("hex");

  if (signature !== expectedSignature) return false;

  // Check if token is not older than 7 days
  const tokenTime = parseInt(timestamp, 10);
  const now = Date.now();
  const maxAge = COOKIE_MAX_AGE * 1000;

  return now - tokenTime < maxAge;
}

export async function POST(request: NextRequest) {
  const password = process.env.DASHBOARD_PASSWORD;
  const secret = process.env.SESSION_SECRET || password;

  if (!password) {
    return NextResponse.json(
      { error: "Server configuration error" },
      { status: 500 }
    );
  }

  try {
    const body = await request.json();
    const { password: inputPassword } = body;

    if (inputPassword !== password) {
      return NextResponse.json({ error: "סיסמה שגויה" }, { status: 401 });
    }

    // Create session token
    const token = createSessionToken(secret!);

    // Set cookie
    const cookieStore = await cookies();
    cookieStore.set(COOKIE_NAME, token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
      maxAge: COOKIE_MAX_AGE,
      path: "/",
    });

    return NextResponse.json({ success: true });
  } catch {
    return NextResponse.json({ error: "Invalid request" }, { status: 400 });
  }
}

// Logout endpoint
export async function DELETE() {
  const cookieStore = await cookies();
  cookieStore.delete(COOKIE_NAME);
  return NextResponse.json({ success: true });
}

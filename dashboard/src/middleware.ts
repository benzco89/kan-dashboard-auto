import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const COOKIE_NAME = "kan_dashboard_session";
const COOKIE_MAX_AGE = 60 * 60 * 24 * 7 * 1000; // 7 days in ms

async function verifySessionToken(
  token: string,
  secret: string
): Promise<boolean> {
  const [timestamp, signature] = token.split(".");
  if (!timestamp || !signature) return false;

  // Use Web Crypto API (Edge Runtime compatible)
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const signatureBuffer = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(timestamp)
  );

  const expectedSignature = Array.from(new Uint8Array(signatureBuffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

  if (signature !== expectedSignature) return false;

  // Check if token is not older than 7 days
  const tokenTime = parseInt(timestamp, 10);
  const now = Date.now();

  return now - tokenTime < COOKIE_MAX_AGE;
}

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Allow login page and API routes
  if (
    pathname === "/login" ||
    pathname.startsWith("/api/auth") ||
    pathname.startsWith("/_next") ||
    pathname.includes(".")
  ) {
    return NextResponse.next();
  }

  const password = process.env.DASHBOARD_PASSWORD;
  const secret = process.env.SESSION_SECRET || password;

  // If no password is set, allow access (dev mode or disabled auth)
  if (!password) {
    return NextResponse.next();
  }

  const token = request.cookies.get(COOKIE_NAME)?.value;

  // Verify token
  const isValid = token ? await verifySessionToken(token, secret!) : false;
  if (!isValid) {
    const loginUrl = new URL("/login", request.url);
    return NextResponse.redirect(loginUrl);
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    /*
     * Match all request paths except:
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     */
    "/((?!_next/static|_next/image|favicon.ico).*)",
  ],
};

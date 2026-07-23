import { Suspense } from "react";
import { AuthForm } from "@/components/auth-form";
export default function VerifyEmailPage() { return <Suspense><AuthForm mode="verify" /></Suspense>; }

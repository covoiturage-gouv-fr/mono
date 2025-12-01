"use client";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

export default function Activite() {
  const router = useRouter();

  useEffect(() => {
    router.push("/activite/export");
  }, [router]);

  return null;
}

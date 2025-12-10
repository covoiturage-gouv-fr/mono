"use client";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

export default function Administration() {
  const router = useRouter();

  useEffect(() => {
    router.push("/administration/profil");
  }, [router]);

  return null;
}

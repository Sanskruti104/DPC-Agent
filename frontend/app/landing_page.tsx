"use client";

import { useRouter } from "next/navigation";

export default function LandingPage() {
  const router = useRouter();

  const handleStartAnalysis = () => {
    router.push("/dashboard");
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-white text-center px-4 selection:bg-black selection:text-white">
      {/* Background radial gradient for modern feel */}
      <div className="absolute inset-0 -z-10 bg-[radial-gradient(circle_at_center,_var(--tw-gradient-stops))] from-gray-50 via-white to-white opacity-40"></div>
      
      <main className="max-w-4xl w-full flex flex-col items-center justify-center animate-in fade-in slide-in-from-bottom-4 duration-1000">
        {/* Title */}
        <h1 className="text-4xl md:text-6xl font-bold text-black tracking-tight leading-tight">
          AI Data Policy <br className="hidden md:block" />
          <span className="text-gray-800">Compliance Agent</span>
        </h1>

        {/* Subtitle */}
        <p className="mt-6 text-lg md:text-xl text-gray-600 max-w-2xl mx-auto leading-relaxed">
          Upload data &rarr; Map schema &rarr; Detect violations &rarr; Get insights
        </p>

        {/* CTA Button */}
        <button
          onClick={handleStartAnalysis}
          className="mt-10 px-8 py-4 rounded-xl bg-black text-white hover:bg-gray-800 active:scale-95 transition-all duration-200 font-medium text-lg flex items-center group shadow-xl shadow-black/10"
        >
          Start Analysis
          <span className="ml-2 group-hover:translate-x-1 transition-transform">&rarr;</span>
        </button>

        {/* Feature badges (Minimalist footer-like section) */}
        <div className="mt-20 flex flex-wrap justify-center gap-6 opacity-40 grayscale pointer-events-none">
          <span className="text-xs uppercase tracking-widest font-semibold border border-gray-200 px-3 py-1 rounded">Dataset Ingestion</span>
          <span className="text-xs uppercase tracking-widest font-semibold border border-gray-200 px-3 py-1 rounded">Explainability Engine</span>
          <span className="text-xs uppercase tracking-widest font-semibold border border-gray-200 px-3 py-1 rounded">Violation Enforcement</span>
        </div>
      </main>
    </div>
  );
}

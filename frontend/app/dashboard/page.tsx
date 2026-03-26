"use client";

import React, { useState } from "react";
import { Upload, FileText, Loader2, AlertCircle, CheckCircle2 } from "lucide-react";

export default function PolicyExtractionDashboard() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [clauses, setClauses] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const file = e.target.files[0];
      if (file.type !== "application/pdf") {
        setError("Only PDF files are supported for policy extraction.");
        setSelectedFile(null);
        return;
      }
      setSelectedFile(file);
      setError(null);
      setClauses([]);
    }
  };

  const handleExtract = async () => {
    if (!selectedFile) return;

    setLoading(true);
    setError(null);
    setClauses([]);

    const formData = new FormData();
    formData.append("file", selectedFile);

    try {
      const response = await fetch("http://localhost:8000/api/v1/policy/extract", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        throw new Error(`Extraction failed: ${response.statusText}`);
      }

      const data = await response.json();
      setClauses(data.clauses || []);
    } catch (err: any) {
      setError(err.message || "An unexpected error occurred during extraction.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-3xl mx-auto">
        {/* SECTION 1 — HEADER */}
        <div className="text-center mb-10">
          <h1 className="text-3xl font-bold text-gray-900 sm:text-4xl">
            Policy Clause Extraction
          </h1>
          <p className="mt-3 text-lg text-gray-500">
            Upload a policy document to extract machine-readable clauses using AI.
          </p>
        </div>

        {/* SECTION 2 — FILE UPLOAD & ACTION */}
        <div className="bg-white shadow-sm rounded-2xl p-8 border border-gray-100">
          <div className="flex flex-col items-center">
            {/* Upload Box */}
            <div className={`w-full border-2 border-dashed rounded-xl p-10 flex flex-col items-center justify-center transition-all ${
              selectedFile ? "border-black bg-gray-50" : "border-gray-200 hover:border-gray-300"
            }`}>
              <div className="bg-gray-100 p-4 rounded-full mb-4">
                <Upload className={`w-8 h-8 ${selectedFile ? "text-black" : "text-gray-400"}`} />
              </div>
              
              <label className="cursor-pointer text-center">
                <span className="text-gray-900 font-medium hover:text-gray-700 transition">
                  {selectedFile ? selectedFile.name : "Select policy PDF"}
                </span>
                <input
                  type="file"
                  className="hidden"
                  accept=".pdf"
                  onChange={handleFileChange}
                />
                {!selectedFile && <p className="text-sm text-gray-400 mt-1">Drag and drop or click to browse</p>}
              </label>
            </div>

            {/* Error Message */}
            {error && (
              <div className="mt-4 flex items-center text-red-600 text-sm bg-red-50 px-4 py-2 rounded-lg w-full">
                <AlertCircle className="w-4 h-4 mr-2" />
                {error}
              </div>
            )}

            {/* SECTION 3 — ACTION */}
            <button
              onClick={handleExtract}
              disabled={!selectedFile || loading}
              className={`mt-8 w-full py-4 rounded-xl font-bold text-lg flex items-center justify-center transition-all ${
                !selectedFile || loading
                  ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                  : "bg-black text-white hover:bg-gray-800 shadow-lg shadow-black/10 active:scale-95"
              }`}
            >
              {loading ? (
                <>
                  <Loader2 className="w-5 h-5 mr-3 animate-spin" />
                  Extracting Clauses...
                </>
              ) : (
                "Extract Clauses"
              )}
            </button>
          </div>
        </div>

        {/* SECTION 4 — OUTPUT */}
        {(clauses.length > 0 || loading) && (
          <div className="mt-12 animate-in fade-in slide-in-from-top-4 duration-500">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-xl font-bold text-gray-900 flex items-center">
                <FileText className="w-5 h-5 mr-2" />
                Extracted Clauses
              </h2>
              {clauses.length > 0 && !loading && (
                <span className="text-xs bg-green-100 text-green-700 px-3 py-1 rounded-full font-bold flex items-center uppercase tracking-wider">
                  <CheckCircle2 className="w-3 h-3 mr-1" />
                  {clauses.length} Clauses Found
                </span>
              )}
            </div>

            <div className="space-y-4">
              {loading ? (
                // Skeleton UI while loading
                [1, 2, 3].map((i) => (
                  <div key={i} className="bg-gray-100 h-20 rounded-xl animate-pulse" />
                ))
              ) : (
                clauses.map((clause, index) => (
                  <div
                    key={index}
                    className="p-5 bg-white rounded-xl border border-gray-100 shadow-sm hover:shadow-md transition-all group flex items-start"
                  >
                    <div className="mr-4 mt-1">
                      <div className="w-6 h-6 rounded-full bg-gray-900 text-white text-xs flex items-center justify-center font-bold">
                        {index + 1}
                      </div>
                    </div>
                    <p className="text-gray-700 leading-relaxed font-medium">
                      {clause}
                    </p>
                  </div>
                ))
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

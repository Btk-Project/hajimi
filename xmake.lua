add_rules("mode.debug", "mode.release")
set_languages("c++23")
set_encodings("utf-8")

option("quic")
    set_default(false)
    set_showmenu(true)
    set_description("Enable QUIC transport")
option_end()

option("lto")
    set_default(false)
    set_showmenu(true)
    set_description("Enable Link Time Optimization")
option_end()

-- Update
add_rules("plugin.compile_commands.autoupdate", {outputdir = "build", lsp = "clangd"})

-- Import async runtime
add_repositories("btk-repo https://github.com/Btk-Project/xmake-repo.git")
add_requires("ilias dev", {configs = {static = true}})

-- Import json, argparse, etc...
add_requires("nlohmann_json", "argparse")

-- Import gtest if debug
if is_mode("debug") then
    add_requires("gtest")
end

-- Quic
if has_config("quic") then
    includes("packages/ngtcp2-quic/xmake.lua")
    add_requires("ngtcp2-quic 1.25.0")
end

-- LTO
if has_config("lto") then
    set_policy("build.optimization.lto", true)
end

target("hajimi")
    set_kind("binary")
    add_packages("ilias")
    add_packages("nlohmann_json", "argparse")
    add_files("src/*.cpp")

    if has_config("quic") then
        add_packages("ngtcp2-quic")
        add_defines("HAJIMI_QUIC")
        add_files("src/quic/*.cpp")
    end

    -- For static resource
    add_rules("utils.bin2obj", {extensions = ".html"})
    add_files("static/index.html")

    if is_mode("release") then
        add_rules("c++.unity_build")
    end

    -- Unit test for debug mode
    if is_mode("debug") then
        add_packages("gtest")
        add_files("./tests/*.cpp")
        if has_config("quic") then
            add_defines('HAJIMI_QUIC_TEST_DIR="' .. path.absolute("tests/certs"):gsub("\\", "/") .. '"')
        end
        add_tests("unit", {runargs = {"--test"}, timeout = 60000})
    end
target_end()

-- Benchmark for quic
if has_config("quic") then
    target("quic-bench")
        set_kind("binary")
        set_default(false)
        add_packages("ilias", "ngtcp2-quic")
        add_files("benchmarks/quic.cpp", "src/quic/*.cpp")
    target_end()
end

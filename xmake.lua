add_rules("mode.debug", "mode.release")
set_languages("c++23")
set_encodings("utf-8")

-- Update
add_rules("plugin.compile_commands.autoupdate", {outputdir = "build", lsp = "clangd"})

-- Import async runtime
add_repositories("btk-repo https://github.com/Btk-Project/xmake-repo.git")
add_requires("ilias 0.5.1", {configs = {static = true, modules = true}})

-- Import json, argparse, etc...
add_requires("nlohmann_json v3.12.0", "argparse v3.2")

option("tests")
    set_default(false)
    set_showmenu(true)
    set_description("Build module regression tests")
option_end()

target("hajimi")
    set_kind("binary")
    add_packages("ilias")
    add_packages("nlohmann_json", "argparse")
    add_files("src/**.cpp", "src/**.cppm")

    -- For static resource
    add_rules("utils.bin2obj", {extensions = ".html"})
    add_files("static/index.html")
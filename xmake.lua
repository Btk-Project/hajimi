add_rules("mode.debug", "mode.release")
set_languages("c++23")
set_encodings("utf-8")

-- Update
add_rules("plugin.compile_commands.autoupdate", {outputdir = "build"})

-- Import async runtime
add_repositories("btk-repo https://github.com/Btk-Project/xmake-repo.git")
add_requires("ilias")

-- Import json, argparse, etc...
add_requires("nlohmann_json", "argparse")

target("hajimi")
    set_kind("binary")
    add_packages("ilias")
    add_packages("nlohmann_json", "argparse")
    add_files("src/*.cpp")

    -- For static resource
    add_rules("utils.bin2obj", {extensions = ".html"})
    add_files("static/index.html")

    if is_mode("release") then
        add_rules("c++.unity_build")
    end
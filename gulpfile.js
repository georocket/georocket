var asciidoctor = require("gulp-asciidoctor");
var del = require("del");
var gulp = require("gulp");
var livereload = require("gulp-livereload");

gulp.task("build", function() {
  return gulp.src("src/**/*.adoc")
    .pipe(asciidoctor({
      base_dir: "src"
    }))
    .pipe(gulp.dest("build"))
    .pipe(livereload());
});

gulp.task("watch", ["build"], function() {
  livereload.listen();
  return gulp.watch(["src/**/*.adoc"], ["build"]);
});

gulp.task("clean", function(cb) {
  del(["build"], cb);
});

gulp.task("default", ["build"]);

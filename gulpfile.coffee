# Load all required libraries.
gulp = require 'gulp'
gutil = require 'gulp-util'
coffee = require 'gulp-coffee'
istanbul = require 'gulp-istanbul'
mocha = require 'gulp-mocha'
changed = require 'gulp-changed'
moment = require 'moment'

COFFEE_SRC = './src/**/*.coffee'
COFFEE_DEST = './lib/'
JS_SRC = 'lib/**/*.js'

gulp.task 'coffee', ->
  gulp.src COFFEE_SRC
  .pipe changed COFFEE_DEST
  .pipe coffee({bare: true}).on('error', gutil.log)
  .pipe gulp.dest COFFEE_DEST


gulp.task 'test', ['coffee'], ->
  gulp.src [JS_SRC]
  .pipe(istanbul())# 覆盖率
  .on 'finish', ->
    gulp.src(['test/**/*.spec.coffee'])
    .pipe mocha reporter: 'spec', compilers: 'coffee:coffee-script'
    .pipe istanbul.writeReports() # 生成覆盖率文件

gulp.task 'watch', ->
  watcher = gulp.watch COFFEE_SRC, ['coffee']
  watcher.on 'change', (event)->
    console.log event.path.replace __dirname, ''
    console.log moment(new Date()).format 'HH:mm:ss'

gulp.task 'default', ['coffee', 'watch']




# sed
sed 's/\$//g' train > train2
perl -np -e 's/\0//g' train2 > train3
# sed 's/:0:/:0.999:/g' train3 > train4
# sed 's/:1:/:0.001:/g' train4 > train5
sed 's/:1:/:-1:/g' train3 > train4
sed 's/:0:/:1:/g' train4 > train5



# 0.32, 0.08
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 1 -f model -p pred_train
./vw -d train5 -t -i model -p pred_train_as_test
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c

# 0.47, 0.02 best ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 2 -f model -p pred_train
./vw -d train5 -t -i model
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c

# 0.180296, 0.005
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 3 -f model -p pred_train
./vw -d train5 -t -i model
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c

# 0.04, 0.01
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 4 -f model -p pred_train
./vw -d train5 -t -i model
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c

# 0.04, 0.01
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 5 -f model -p pred_train
./vw -d train5 -t -i model
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c

# sed
sed 's/\$//g' test > test2
perl -np -e 's/\0//g' test2 > test3
# sed 's/:0:/:0.999:/g' test3 > test4
# sed 's/:1:/:0.001:/g' test4 > test5
sed 's/:1:/:-1:/g' test3 > test4
sed 's/:0:/:1:/g' test4 > test5



# 0.32, 0.08
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 1 -f model -p pred_train
./vw -d train5 -t -i model -p pred_train_as_test
./vw -d test5 -t -i model -p pred_test
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c
cut -f 1 -d" " pred_test | sort -rn | uniq -c

cp pred_train /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_1/pred_train
cp pred_train_as_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_1/pred_train_as_test
cp pred_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_1/pred_test

# 0.47, 0.02 best ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 2 -f model -p pred_train
./vw -d train5 -t -i model -p pred_train_as_test
./vw -d test5 -t -i model -p pred_test
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c
cut -f 1 -d" " pred_test | sort -rn | uniq -c

cp pred_train /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_2/pred_train
cp pred_train_as_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_2/pred_train_as_test
cp pred_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_2/pred_test

# 0.180296, 0.005
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 3 -f model -p pred_train
./vw -d train5 -t -i model -p pred_train_as_test
./vw -d test5 -t -i model -p pred_test
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c
cut -f 1 -d" " pred_test | sort -rn | uniq -c

cp pred_train /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_3/pred_train
cp pred_train_as_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_3/pred_train_as_test
cp pred_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_3/pred_test

# 0.04, 0.01
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 4 -f model -p pred_train
./vw -d train5 -t -i model -p pred_train_as_test
./vw -d test5 -t -i model -p pred_test
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c
cut -f 1 -d" " pred_test | sort -rn | uniq -c

cp pred_train /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_4/pred_train
cp pred_train_as_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_4/pred_train_as_test
cp pred_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_4/pred_test

# 0.04, 0.01
./vw -d train5 --cb_adf --cb_type dr --quadratic si --quadratic ai --quadratic ss --quadratic aa --cubic sai --feature_limit 5 -f model -p pred_train
./vw -d train5 -t -i model -p pred_train_as_test
./vw -d test5 -t -i model -p pred_test
cut -f 1 -d" " pred_train | sort -rn | uniq -c
cut -f 1 -d" " pred_train_as_test | sort -rn | uniq -c
cut -f 1 -d" " pred_test | sort -rn | uniq -c

cp pred_train /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_5/pred_train
cp pred_train_as_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_5/pred_train_as_test
cp pred_test /Users/lcui/study_toy_examples/titanic/titanic/vw_data_negative1_positive1_format/feature_limit_5/pred_test

# Git documentation

This is the guide for the minidb git strategy.

##Branches

* `master` : this is the main release branch.
Develop will be merged into it at the end of a sprint.

* `develop` : this is the main branch of each sprint.
It is merged into develop at the end of the sprint.

* `feature-*` : this is the working branch of each feature.
There is a tight coupling between it and the corresponding issue.
It is created from develop and at the end of its lifecycle is merged back into it.
After the successful merge of develop into master all of the merged feature branches will be deleted.

* `bug-*` : this is the working branch for any bugs into develop.
It is created from develop and at the end of its lifecycle is merged back into it.
A issue must be present, tight coupled with the branch and well documented.

* `hotfix-*` : this is the working branch for any bugs into master.
It is created from master and at the end of its lifecycle is merged back into it.
Before and after this branch, archival branches are created to capture the state of develop.
A issue must be present, tight coupled with the branch and well documented.

* `archive-stable-DD-MM-YY` : this is the archive branch of master at the most stable phase.
It is made after the develop is merged and extensively tested. It is also made before and after each hotfix
with the name of the hotfix at the end prepended by BEFORE or AFTER.

* `archive-develop-DD-MM-YY` : this is the archive branch of develop at the most stable phase.
It is made after the develop is merged into master. It is also made before and after each bug
with the name of the bug at the end prepended by BEFORE or AFTER.
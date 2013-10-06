.PHONY: flake8

flake8:
	rm -Rf cache/email-changes-templates/
	flake8


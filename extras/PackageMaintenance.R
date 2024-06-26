# @file PackageMaintenance
#
# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Format and check code
OhdsiRTools::formatRFolder("./R") #(note: this function has been impacted by change in formatR)
OhdsiRTools::checkUsagePackage("CohortGenerator")
OhdsiRTools::updateCopyrightYearFolder()
devtools::spell_check()
styler::style_pkg()
devtools::document()

# Create manual and vignettes:
unlink("extras/CohortGenerator.pdf")
shell("R CMD Rd2pdf ./ --output=extras/CohortGenerator.pdf")

dir.create(path = "./inst/doc/", showWarnings = FALSE)
rmarkdown::render("vignettes/GeneratingCohorts.Rmd",
                  output_file = "../inst/doc/GeneratingCohorts.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

rmarkdown::render("vignettes/CreatingCohortSubsetDefinitions.Rmd",
                  output_file = "../inst/doc/CreatingCohortSubsetDefinitions.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

rmarkdown::render("vignettes/SamplingCohorts.Rmd",
                  output_file = "../inst/doc/SamplingCohorts.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

unloadNamespace("CohortGenerator")
pkgdown::build_site()
OhdsiRTools::fixHadesLogo()

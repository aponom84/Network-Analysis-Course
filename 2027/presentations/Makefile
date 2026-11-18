# Сборка презентаций курса: make (всё), make template, make lecture.
# Каждая презентация -- папка с одноимённым .tex; тема берётся из theme/.
LATEXMK = TEXINPUTS=../theme//: latexmk -lualatex -interaction=nonstopmode -halt-on-error
DECKS = template/template Optimal-Logistics-Network/Optimal-Logistics-Network

.PHONY: all template lecture clean
all: $(addsuffix .pdf,$(DECKS))
template: template/template.pdf
lecture: Optimal-Logistics-Network/Optimal-Logistics-Network.pdf

%.pdf: %.tex theme/beamerthemeHSEnet.sty
	cd $(dir $<) && $(LATEXMK) $(notdir $<)

# удалить вспомогательные файлы LaTeX (PDF остаются)
clean:
	for d in $(DECKS); do (cd `dirname $$d` && latexmk -c `basename $$d`.tex); done

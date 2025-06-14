" vimdiff configuration for choosing between existing and importing text
" for example description or manufacturer or wahetever
" TEMP_DIR LEFT_NAME and RIGHT_NAME must be replaced with proper
" directory or file, use from string import Template

:set nocompatible

function! OpenDiffWithHelp()
	" Arrange windows:
	" help window on bottom, no modifiable
	" on top two compare panels
	execute 'silent edit ${TEMP_DIR}vimdiff_help.txt'
	execute 'diffoff'
	let width = winwidth(0)
	execute '%center ' . width
	setlocal buftype=nofile
	setlocal bufhidden=wipe
	setlocal noswapfile
	setlocal nomodifiable
	execute 'silent split ${LEFT_NAME}'
	execute 'diffthis'
	execute 'silent vsplit ${RIGHT_NAME}'
	execute 'diffthis'
endfunction

function! PickOption_vmode(n)
	" takes nth option from line like this:
	" opt1 | opt2 | opt3 | ....


	" if no option, just get line from right panel
	if a:n == 0
	   execute "'<,'>diffget"
	   return
	endif
	
	" for option selection to work, only one line can be selected
	if line("'<") != line("'>")
	   echo "Only one line can be selected when picking option"
	   return
	endif
	
	execute "'<,'>diffget"
	let line = getline('.')

	" Split on '|', trim whitespace
	let options = map(split(line, '|'), 'trim(v:val)')

	" Get the option (1-based indexing)
	let choice = get(options, a:n - 1, '')

	" Paste the chosen option
	call setline('.',choice)

	" clear defoult register, just in case
	" when user press p in normal mode
	call setreg("","")
endfunction
function! PickOption_nmode(n)
	" takes nth option from line like this:
	" opt1 | opt2 | opt3 | ....

	" Get the current line if in normal mode
	" mode() do not work in this context
	" so checking if start and end in the same col
	let line_number = line('.')
	call setpos("'<",[0,line_number,1,0])
	call setpos("'>",[0,line_number,1,0])

	execute "'<,'>diffget"
	" if no option, just get line from right panel
	if a:n == 0
	   return
	endif
	
	
	let line = getline('.')

	" Split on '|', trim whitespace
	let options = map(split(line, '|'), 'trim(v:val)')

	" Get the option (1-based indexing)
	let choice = get(options, a:n - 1, '')

	" Paste the chosen option
	call setline('.',choice)

	" clear defoult register, just in case
	" when user press p in normal mode
	call setreg("","")
endfunction

let mapleader = " "

autocmd VimEnter * call OpenDiffWithHelp()

" Map the <SPACE>pn key with pick function 
" (take diffrence from other panel)
" possibly may take option if available
nnoremap <silent> <Leader>p  :<C-u>call PickOption_nmode(0)<CR>
nnoremap <silent> <Leader>p1 :<C-u>call PickOption_nmode(1)<CR>
nnoremap <silent> <Leader>p2 :<C-u>call PickOption_nmode(2)<CR>
nnoremap <silent> <Leader>p3 :<C-u>call PickOption_nmode(3)<CR>
nnoremap <silent> <Leader>p4 :<C-u>call PickOption_nmode(4)<CR>
nnoremap <silent> <Leader>p5 :<C-u>call PickOption_nmode(5)<CR>
nnoremap <silent> <Leader>p6 :<C-u>call PickOption_nmode(6)<CR>
nnoremap <silent> <Leader>p7 :<C-u>call PickOption_nmode(7)<CR>
nnoremap <silent> <Leader>p8 :<C-u>call PickOption_nmode(8)<CR>
nnoremap <silent> <Leader>p9 :<C-u>call PickOption_nmode(9)<CR>

xnoremap <silent> <Leader>p  :<C-u>call PickOption_vmode(0)<CR>
xnoremap <silent> <Leader>p1 :<C-u>call PickOption_vmode(1)<CR>
xnoremap <silent> <Leader>p2 :<C-u>call PickOption_vmode(2)<CR>
xnoremap <silent> <Leader>p3 :<C-u>call PickOption_vmode(3)<CR>
xnoremap <silent> <Leader>p4 :<C-u>call PickOption_vmode(4)<CR>
xnoremap <silent> <Leader>p5 :<C-u>call PickOption_vmode(5)<CR>
xnoremap <silent> <Leader>p6 :<C-u>call PickOption_vmode(6)<CR>
xnoremap <silent> <Leader>p7 :<C-u>call PickOption_vmode(7)<CR>
xnoremap <silent> <Leader>p8 :<C-u>call PickOption_vmode(8)<CR>
xnoremap <silent> <Leader>p9 :<C-u>call PickOption_vmode(9)<CR>


" coloring...anything but standard
highlight DiffAdd    cterm=bold ctermfg=10 ctermbg=17 gui=none guifg=bg guibg=Red
highlight DiffDelete cterm=bold ctermfg=10 ctermbg=17 gui=none guifg=bg guibg=Red
highlight DiffChange cterm=bold ctermfg=10 ctermbg=17 gui=none guifg=bg guibg=Red
highlight DiffText   cterm=bold ctermfg=10 ctermbg=88 gui=none guifg=bg guibg=Red

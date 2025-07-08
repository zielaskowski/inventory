" vimdiff configuration for choosing between existing and importing text
" for example description or manufacturer or wahetever
" variables, use with jinja2 Template :
" TEMP_DIR      directory, preferably /tmp/
" COLUMN
" LEFT_NAME     file name (without extension)
" RIGHT_NAME 

:set nocompatible
" disable filler lines, compare line by line
":set diffopt+=context:0
"execute 'diffupdate'

function! OpenDiffWithHelp()
        " Arrange windows:
        " help window on bottom, no modifiable
        " on top one helper and two compare panels
        execute 'silent edit {{TEMP_DIR}}vimdiff_help.txt'
        execute 'diffoff'
        let width = winwidth(0)
        execute '%center ' . width
        setlocal buftype=nofile
        setlocal bufhidden=wipe
        setlocal noswapfile
        setlocal nomodifiable
        execute 'silent split {{TEMP_DIR}}{{REF_COL}}.txt'
        execute 'diffoff'
	setlocal scrollbind
        setlocal buftype=nofile
        setlocal bufhidden=wipe
        setlocal noswapfile
        setlocal nomodifiable
        execute 'silent vsplit {{TEMP_DIR}}{{LEFT_NAME}}.txt'
        execute 'diffthis'
        execute 'silent vsplit {{TEMP_DIR}}{{RIGHT_NAME}}.txt'
        execute 'diffthis'
	setlocal scrollbind
	execute 'call cursor ({{START_LINE}},1)'
endfunction

function! PickOption_vmode()
        " takes from right panel for selected lines
           execute "'<,'>diffget"
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

	" assign func argument to variable, other way not modifable
	let opt = a:n

        execute "'<,'>diffget"

        let line = getline('.')

        " Split on '|', trim whitespace
        let options = map(split(line, '|'), 'trim(v:val)')

	" limit slection to available options
	if len(options) < opt +1
		let opt = len(options) -1
	endif

	" get line number
	let line_no = get(options,0,'')

        " Get the option (1-based indexing)
        let choice = get(options, opt , '')

        " Paste the chosen option
        call setline('.',line_no .. '| ' .. choice)

        " clear defoult register, just in case
        " when user press p in normal mode
        call setreg("","")
endfunction

function! BufferChange()
      " just write changes and exit
      wqa
endfunction

let mapleader = " "

autocmd VimEnter * call OpenDiffWithHelp()
{% if EXIT_ON_CHANGE %}
autocmd TextChanged * call BufferChange()
{% endif %}

" Abort by emptying the file
nnoremap Q :%d<CR>:wqa<CR>
" windowish save and exit shortcuts
nnoremap <C-s> :wqa<CR>
nnoremap <C-c> :qa!<CR>

" Map the <SPACE>pn key with pick function 
" (take diffrence from other panel)
" possibly may take option if available
nnoremap <silent> <Leader>p1 :<C-u>call PickOption_nmode(1)<CR>
nnoremap <silent> <Leader>p2 :<C-u>call PickOption_nmode(2)<CR>
nnoremap <silent> <Leader>p3 :<C-u>call PickOption_nmode(3)<CR>
nnoremap <silent> <Leader>p4 :<C-u>call PickOption_nmode(4)<CR>
nnoremap <silent> <Leader>p5 :<C-u>call PickOption_nmode(5)<CR>
nnoremap <silent> <Leader>p6 :<C-u>call PickOption_nmode(6)<CR>
nnoremap <silent> <Leader>p7 :<C-u>call PickOption_nmode(7)<CR>
nnoremap <silent> <Leader>p8 :<C-u>call PickOption_nmode(8)<CR>
nnoremap <silent> <Leader>p9 :<C-u>call PickOption_nmode(9)<CR>

xnoremap <silent> <Leader>p  :<C-u>call PickOption_vmode()<CR>



" coloring...anything but standard
highlight DiffAdd    cterm=bold ctermfg=10 ctermbg=17 gui=none guifg=bg guibg=Red
highlight DiffDelete cterm=bold ctermfg=10 ctermbg=17 gui=none guifg=bg guibg=Red
highlight DiffChange cterm=bold ctermfg=10 ctermbg=17 gui=none guifg=bg guibg=Red
highlight DiffText   cterm=bold ctermfg=10 ctermbg=88 gui=none guifg=bg guibg=Red
